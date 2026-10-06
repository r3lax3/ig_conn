-- an Account (Instagram ID) belongs to one Source at most
create unique index sources_account on sources (channel_type, external_account_id);

-- the login that worked: a reconnect may come without one (contract 5.5)
alter table sources add column login text;
