# Домашнее задание №5 · Online Cinema

Миняйцев Иван

Сервис на Python принимает события онлайн-кинотеатра и публикует их в Kafka. ClickHouse читает Avro-события через Kafka Engine и сохраняет их в MergeTree. Сервис агрегации рассчитывает дневные метрики в ClickHouse, сохраняет готовые значения в PostgreSQL и выгружает их в S3-совместимое хранилище SeaweedFS. Grafana читает агрегаты из ClickHouse.

## Запуск

Нужен Docker с Compose v2.

```bash
docker compose up
```

При старте Compose поднимает все сервисы, применяет SQL-миграции, создаёт топик и регистрирует схему. Генератор загружает демонстрационные события за предыдущие 14 дней и каждые 3 секунды публикует новую сессию. Графики заполняются по мере загрузки событий и пересчёта метрик.

| Сервис | Адрес |
| --- | --- |
| Movie Service, Swagger | [localhost:8000/docs](http://localhost:8000/docs) |
| Aggregation Service, Swagger | [localhost:8001/docs](http://localhost:8001/docs) |
| Schema Registry, версия 1 | [localhost:8081/subjects/movie-events-value/versions/1](http://localhost:8081/subjects/movie-events-value/versions/1) |
| Grafana, дашборд Online Cinema | [localhost:3000/d/cinema-analytics/online-cinema](http://localhost:3000/d/cinema-analytics/online-cinema) |
| SeaweedFS, S3 API | [localhost:8333](http://localhost:8333) · `cinema` / `cinema-storage` |

`GENERATOR_INTERVAL_SECONDS` управляет интервалом между сессиями, `AGGREGATION_INTERVAL_SECONDS` — интервалом между автоматическими циклами пересчёта (по умолчанию 60 секунд). Значение `0` отключает соответствующий фоновый процесс. Переменные можно передать при запуске или записать в локальный `.env`.

## Публикация и пересчёт

```bash
stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
day=${stamp%%T*}

curl -fsS http://localhost:8000/events \
  -H 'Content-Type: application/json' --data-binary @- <<JSON
{
  "user_id": "demo-user",
  "movie_id": "orbit",
  "event_type": "VIEW_STARTED",
  "timestamp": "$stamp",
  "device_type": "TV",
  "session_id": "demo-session-$stamp",
  "progress_seconds": 0
}
JSON

curl -fsS -X POST "http://localhost:8001/aggregate/$day"
curl -fsS -X POST "http://localhost:8001/export/$day"
curl -fsS --aws-sigv4 'aws:amz:us-east-1:s3' -u cinema:cinema-storage \
  "http://localhost:8333/movie-analytics/daily/$day/aggregates.json"
```

`POST /events` возвращает UUID и статус `201` после подтверждения Kafka. Если UUID отсутствует в запросе, сервис создаёт его. Для событий просмотра нужен целочисленный прогресс от 0 секунд, для поиска и лайка его можно оставить пустым. Время события приводится к UTC с точностью до миллисекунд. Ошибка валидации даёт `422`, ошибка доставки — `503`. ClickHouse читает сообщения асинхронно, поэтому ручной пересчёт сразу после публикации может понадобиться повторить через несколько секунд.

Схема [MovieEvent v1](schemas/movie-event.v1.avsc) регистрируется под именем `movie-events-value` с проверкой `BACKWARD_TRANSITIVE`. Ключ Kafka — `user_id`, поэтому события одного пользователя попадают в одну партицию и сохраняют порядок публикации. У топика 3 партиции, 2 реплики и `min.insync.replicas=1`. Продюсер использует `acks=all`, идемпотентную запись и экспоненциальную задержку повторов. Три совмещённых узла KRaft сохраняют кворум при остановке одного узла.

## Метрики и экспорт

| Метрика | Расчёт за день UTC |
| --- | --- |
| DAU | Уникальные пользователи с любым событием |
| Среднее время просмотра | Среднее `progress_seconds` у событий `VIEW_FINISHED` |
| Топ фильмов | 10 фильмов по числу `VIEW_STARTED` |
| Конверсия | `VIEW_FINISHED / VIEW_STARTED` за тот же день |
| Retention D0–D7 | Когорта формируется в дату первого `VIEW_STARTED`. Возвратом считается любое событие пользователя в соответствующий день UTC |

Отсутствующие значения хранятся как `NULL`. В тепловой карте 0% означает, что день уже наступил и возвратов не было, а прочерк означает, что день ещё не наступил. Текущий день обновляется по мере поступления событий.

ClickHouse хранит raw-события по месяцам UTC с сортировкой по дате, времени и UUID. Запросы расчёта используют `FINAL`, чтобы повторная доставка события с тем же UUID и временем не удваивала метрики. Для каждой даты материализуется дневной снимок в `cinema.daily_snapshots`. PostgreSQL обновляет метрики в одной транзакции по ключу «дата, метрика, измерение».

Планировщик пересчитывает текущую дату и восемь предыдущих, чтобы учитывать поздние события и созревание D7. API принимает любую прошедшую дату или текущий день. Завершённые дни автоматически выгружаются в `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`. Запись за ту же дату заменяет объект. Если метрики изменились, файл обновляется в ближайшем цикле. При ошибке выгрузки дата остаётся в очереди для повтора.

## Проверка

```bash
./scripts/test.sh
```

Скрипт поднимает отдельный Compose-проект, проверяет валидацию и поля событий в ClickHouse, расчёты в ClickHouse и PostgreSQL, запросы панелей Grafana и повторную выгрузку в S3. После остановки одного брокера тест повторно проверяет доставку нового события до ClickHouse. Тестовые контейнеры и тома удаляются после запуска.
