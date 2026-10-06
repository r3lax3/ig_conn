-- a Source's device from before its first successful login (Fernet token): a failed first
-- attempt and the next one must look like the same phone. Once connected, sessions.device rules
create table devices (
    source_id uuid primary key,
    device bytea not null,
    updated_at timestamptz not null
);
