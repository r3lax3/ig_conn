"""Inbox polling through the aiograpi adapter: what is new after the positions, nothing seen."""

from datetime import UTC, datetime, timedelta

import pytest

from ig_connector.instagram import Failure, InboxPositions, InboxUser, PlatformError, ThreadPosition
from tests.adapter.conftest import PEER, THREAD, restored
from tests.support.instagram_api import VIEWER, FakeApi, inbox, item, status, thread, user

SINCE = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return SINCE + timedelta(seconds=seconds)


def mid(n: int) -> str:
    # Instagram item ids: long numbers, growing with time
    return f"3100000000000000000000000000000{n:04d}"


FRESH = InboxPositions(since=SINCE, threads={})


async def test_new_messages_come_oldest_first_with_instagrams_exact_time(api: FakeApi) -> None:
    exact = SINCE + timedelta(seconds=5, microseconds=123456)
    api.on(
        "direct_v2/inbox/",
        inbox(
            [
                thread(
                    THREAD,
                    [user(PEER, "bob", "Bob B")],
                    [
                        item(mid(3), at(9), user_id=VIEWER, text="our answer"),
                        item(mid(2), exact, user_id=PEER, text="second", reply_to=mid(1)),
                        item(mid(1), at(-60), user_id=PEER, text="before the connect"),
                    ],
                )
            ]
        ),
    )
    platform, source_id = await restored(api)

    [found] = await platform.new_messages(source_id, FRESH)

    assert found.thread_id == THREAD
    assert found.users == [InboxUser(PEER, "bob", "Bob B")]
    assert (found.is_group, found.pending) == (False, False)
    assert [m.message_id for m in found.messages] == [mid(2), mid(3)]
    second, ours = found.messages
    assert second.sent_at == exact
    assert (second.sender_id, second.text, second.item_type) == (PEER, "second", "text")
    assert second.reply_to_message_id == mid(1)
    assert not second.is_sent_by_viewer
    assert ours.is_sent_by_viewer


async def test_only_messages_after_the_threads_position(api: FakeApi) -> None:
    items = [item(mid(n), at(n), user_id=PEER) for n in (4, 3, 2, 1)]
    api.on("direct_v2/inbox/", inbox([thread(THREAD, [user(PEER)], items)]))
    platform, source_id = await restored(api)
    positions = InboxPositions(since=SINCE, threads={THREAD: ThreadPosition(mid(2), at(2))})

    [found] = await platform.new_messages(source_id, positions)

    assert [m.message_id for m in found.messages] == [mid(3), mid(4)]


async def test_a_thread_with_nothing_new_is_left_out(api: FakeApi) -> None:
    api.on("direct_v2/inbox/", inbox([thread(THREAD, [user(PEER)], [item(mid(1), at(1), user_id=PEER)])]))
    platform, source_id = await restored(api)
    positions = InboxPositions(since=SINCE, threads={THREAD: ThreadPosition(mid(1), at(1))})

    assert await platform.new_messages(source_id, positions) == []


async def test_a_busy_thread_is_paged_back_to_its_position_without_a_gap(api: FakeApi) -> None:
    # the inbox gives the newest 10; 25 came since the last poll
    newest = [item(mid(n), at(n), user_id=PEER) for n in range(30, 20, -1)]
    api.on(
        "direct_v2/inbox/",
        inbox([thread(THREAD, [user(PEER)], newest, has_older=True, oldest_cursor="c1")]),
    )
    page = [item(mid(n), at(n), user_id=PEER) for n in range(20, 0, -1)]
    api.on(
        f"direct_v2/threads/{THREAD}/",
        {"status": "ok", "thread": thread(THREAD, [user(PEER)], page, has_older=True, oldest_cursor="c2")},
    )
    platform, source_id = await restored(api)
    positions = InboxPositions(since=SINCE, threads={THREAD: ThreadPosition(mid(5), at(5))})

    [found] = await platform.new_messages(source_id, positions)

    assert [m.message_id for m in found.messages] == [mid(n) for n in range(6, 31)]
    [paged] = [c for c in api.calls if c.path == f"direct_v2/threads/{THREAD}/"]
    assert paged.params["cursor"] == "c1"


async def test_more_inbox_pages_only_while_the_oldest_thread_has_news(api: FakeApi) -> None:
    other = "340282366841710300949128000000000002"
    api.on(
        "direct_v2/inbox/",
        inbox(
            [thread(THREAD, [user(PEER)], [item(mid(9), at(9), user_id=PEER)])], has_older=True, cursor="p2"
        ),
        inbox(
            [thread(other, [user("777")], [item(mid(8), at(8), user_id="777")])], has_older=True, cursor="p3"
        ),
    )
    platform, source_id = await restored(api)
    positions = InboxPositions(since=SINCE, threads={other: ThreadPosition(mid(8), at(8))})

    found = await platform.new_messages(source_id, positions)

    assert [t.thread_id for t in found] == [THREAD]
    assert [c.params.get("cursor") for c in api.calls] == [None, "p2"]


async def test_threads_come_oldest_activity_first(api: FakeApi) -> None:
    other = "340282366841710300949128000000000002"
    api.on(
        "direct_v2/inbox/",
        inbox(
            [
                thread(THREAD, [user(PEER)], [item(mid(9), at(9), user_id=PEER)]),
                thread(other, [user("777")], [item(mid(3), at(3), user_id="777")]),
            ]
        ),
    )
    platform, source_id = await restored(api)

    found = await platform.new_messages(source_id, FRESH)

    assert [t.thread_id for t in found] == [other, THREAD]


async def test_groups_and_unknown_items_are_passed_on_as_they_are(api: FakeApi) -> None:
    group = "340282366841710300949128000000000003"
    api.on(
        "direct_v2/inbox/",
        inbox(
            [
                thread(group, [user(PEER), user("777")], [item(mid(2), at(2), user_id=PEER)], is_group=True),
                thread(
                    THREAD,
                    [user(PEER)],
                    [
                        item(mid(4), at(4), user_id=PEER, text=None, item_type="voice_media"),
                        {"item_type": "text", "text": "no id, no time"},
                    ],
                ),
            ]
        ),
    )
    platform, source_id = await restored(api)

    found = {t.thread_id: t for t in await platform.new_messages(source_id, FRESH)}

    assert found[group].is_group
    [voice] = found[THREAD].messages
    assert (voice.item_type, voice.text) == ("voice_media", None)


async def test_reading_the_inbox_marks_nothing_seen(api: FakeApi) -> None:
    newest = [item(mid(n), at(n), user_id=PEER) for n in range(30, 20, -1)]
    api.on(
        "direct_v2/inbox/", inbox([thread(THREAD, [user(PEER)], newest, has_older=True, oldest_cursor="c1")])
    )
    api.on(f"direct_v2/threads/{THREAD}/", {"status": "ok", "thread": thread(THREAD, [user(PEER)], [])})
    platform, source_id = await restored(api)

    await platform.new_messages(source_id, FRESH)

    assert api.calls
    for call in api.calls:
        assert call.method == "GET"
        assert "seen" not in call.path
        assert call.params["visual_message_return_type"] == "unseen"


async def test_the_pending_inbox_is_never_read(api: FakeApi) -> None:
    api.on("direct_v2/inbox/", inbox([]))
    platform, source_id = await restored(api)

    await platform.new_messages(source_id, FRESH)

    assert api.paths() == ["direct_v2/inbox/"]


@pytest.mark.parametrize(
    ("answer", "failure"),
    [
        (status(403, {"status": "fail", "message": "login_required"}), Failure.SESSION_REVOKED),
        (status(400, {"status": "fail", "message": "challenge_required"}), Failure.CHALLENGE),
        (status(400, {"status": "fail", "message": "feedback_required"}), Failure.FLOOD),
        (status(429, {"status": "fail"}), Failure.RATE_LIMITED),
        (status(500, {"status": "fail"}), Failure.REJECTED),
    ],
)
async def test_inbox_refusals(api: FakeApi, answer: object, failure: Failure) -> None:
    api.on("direct_v2/inbox/", answer)  # type: ignore[arg-type]
    platform, source_id = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.new_messages(source_id, FRESH)

    assert refused.value.failure is failure


async def test_a_link_message_carries_its_text(api: FakeApi) -> None:
    link = item(mid(1), at(1), user_id=PEER, text=None, item_type="link")
    link["link"] = {"text": "see https://example.com", "link_context": {"link_url": "https://example.com"}}
    api.on("direct_v2/inbox/", inbox([thread(THREAD, [user(PEER)], [link])]))
    platform, source_id = await restored(api)

    [found] = await platform.new_messages(source_id, FRESH)

    [message] = found.messages
    assert (message.item_type, message.text) == ("link", "see https://example.com")
