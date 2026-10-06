-- where the connector is in each Source's inbox; thread ids never leave the connector
create table inbox_starts (
    source_id uuid primary key references sources (source_id),
    -- the last (re)connect: older messages are history and are not loaded (stage 2)
    since timestamptz not null
);

create table inbox_positions (
    source_id uuid not null references sources (source_id),
    thread_id text not null,
    -- the last message the connector is done with (published or skipped)
    message_id text not null,
    sent_at timestamptz not null,
    updated_at timestamptz not null,
    primary key (source_id, thread_id)
);

-- Sources connected before this migration start their inbox now
insert into inbox_starts (source_id, since) select source_id, now() from sources;
