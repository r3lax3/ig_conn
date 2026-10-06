-- password and TOTP secret are never stored, not even here
create table login_flows (
    operation_id uuid primary key,
    source_id uuid not null,
    channel_type text not null,
    state text not null,
    login text,
    proxy_country_code text,
    proxy_network_type text,
    started_at timestamptz not null,
    expires_at timestamptz not null,
    updated_at timestamptz not null
);

create table sources (
    source_id uuid primary key,
    channel_type text not null,
    external_account_id text not null,
    username text not null,
    full_name text,
    proxy_country_code text not null,
    proxy_network_type text,
    status text,
    status_code text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

-- Fernet tokens (key from env): a database dump does not give the accounts away
create table sessions (
    source_id uuid primary key references sources (source_id),
    session bytea not null,
    device bytea not null,
    updated_at timestamptz not null
);
