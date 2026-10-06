create table operations (
    operation_id uuid not null,
    type text not null,
    source_id uuid not null,
    state text not null,
    result jsonb,
    received_at timestamptz not null,
    updated_at timestamptz not null,
    -- connect.start and connect.confirm share operation_id (contract 11)
    primary key (operation_id, type)
);
