-- a Source whose Account moved to a new Source (it was dead) is kept, switched off, and no
-- longer holds the Account
alter table sources add column disabled_at timestamptz;

drop index sources_account;
create unique index sources_account on sources (channel_type, external_account_id) where disabled_at is null;
