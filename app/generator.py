import logging
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from random import Random
from threading import Event
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.events import DeviceType, EventType, MovieEvent

logger = logging.getLogger("uvicorn.error")
MOVIES = ("northern-lights", "night-train", "signal", "the-last-shore", "glass-city", "orbit")
RETURN_RATES = (1.0, 0.65, 0.48, 0.38, 0.32, 0.26, 0.22, 0.18)
USERS_PER_COHORT = 18


def session_events(
    user_id: str, session_id: str, ended_at: datetime, rng: Random,
) -> Iterator[MovieEvent]:
    movie_id = rng.choices(MOVIES, weights=(22, 18, 15, 12, 9, 7))[0]
    device = rng.choice(tuple(DeviceType))
    duration = rng.randint(2700, 7800)
    delay = 0
    events = []

    if rng.random() < 0.55:
        events.append((EventType.SEARCHED, 0, None))
        delay = rng.randint(5, 60)
    events.append((EventType.VIEW_STARTED, delay, 0))

    if rng.random() < 0.78:
        if rng.random() < 0.50:
            watched = rng.randint(120, duration - 120)
            events.append((EventType.VIEW_PAUSED, delay + watched, watched))
            delay += rng.randint(10, 240)
            events.append((EventType.VIEW_RESUMED, delay + watched, watched))
        events.append((EventType.VIEW_FINISHED, delay + duration, duration))
        if rng.random() < 0.40:
            events.append((EventType.LIKED, delay + duration + rng.randint(1, 45), None))
    else:
        watched = rng.randint(30, duration // 2)
        events.append((EventType.VIEW_PAUSED, delay + watched, watched))

    start = ended_at - timedelta(seconds=events[-1][1])
    for event_type, offset, progress in events:
        yield MovieEvent(
            event_id=uuid5(NAMESPACE_URL, f"{session_id}/{event_type}"),
            user_id=user_id,
            movie_id=movie_id,
            event_type=event_type,
            timestamp=start + timedelta(seconds=offset),
            device_type=device,
            session_id=session_id,
            progress_seconds=progress,
        )


def historical_events(today: date) -> Iterator[MovieEvent]:
    for days_ago in range(14, 0, -1):
        day = today - timedelta(days=days_ago)
        rng = Random(day.isoformat())
        start = datetime.combine(day, time(3), UTC)
        for age, return_rate in enumerate(RETURN_RATES):
            cohort = day - timedelta(days=age)
            for user in range(USERS_PER_COHORT):
                if rng.random() < return_rate:
                    user_id = f"demo-{cohort}-{user:02d}"
                    ended_at = start + timedelta(seconds=rng.randrange(21 * 3600))
                    yield from session_events(user_id, f"{user_id}-{day}", ended_at, rng)


def generate(publish: Callable[[MovieEvent], None], interval: float, stop: Event) -> None:
    rng = Random()
    try:
        for event in historical_events(datetime.now(UTC).date()):
            if stop.is_set():
                return
            publish(event)
        logger.info("demo_history_published")
    except Exception:
        logger.exception("demo_history_publish_failed")

    while not stop.wait(interval):
        now = datetime.now(UTC)
        cohort = now.date() - timedelta(days=rng.randrange(len(RETURN_RATES)))
        user_id = f"demo-{cohort}-{rng.randrange(USERS_PER_COHORT):02d}"
        try:
            for event in session_events(user_id, str(uuid4()), now, rng):
                if stop.is_set():
                    return
                publish(event)
        except Exception:
            logger.exception("demo_session_publish_failed")
