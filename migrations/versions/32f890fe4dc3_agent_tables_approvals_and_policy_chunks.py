"""agent tables, approvals and policy chunks

Revision ID: 32f890fe4dc3
Revises: f0e6560124cf
Create Date: 2026-11-26 08:40:48.252844

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "32f890fe4dc3"
down_revision: str | Sequence[str] | None = "f0e6560124cf"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


EMBEDDING_DIMENSIONS = 256  # small and cheap; OpenAI text-embedding-3-small supports it


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("""
        CREATE TABLE conversations (
            id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            user_id    bigint NOT NULL REFERENCES users (id),
            created_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE agent_runs (
            id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            conversation_id bigint NOT NULL REFERENCES conversations (id),
            user_id         bigint NOT NULL REFERENCES users (id),
            input           text NOT NULL,
            output          text,
            status          text NOT NULL DEFAULT 'queued'
                            CHECK (status IN ('queued', 'running', 'waiting_approval',
                                              'completed', 'failed')),
            tokens_in       integer NOT NULL DEFAULT 0,
            tokens_out      integer NOT NULL DEFAULT 0,
            cost_usd        numeric(12, 6) NOT NULL DEFAULT 0,
            created_at      timestamptz NOT NULL DEFAULT now(),
            updated_at      timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX agent_runs_conversation_id_idx ON agent_runs (conversation_id, id)")
    # ADR 004: one approval covers one hold and amount, is single use and expires with the hold.
    op.execute("""
        CREATE TABLE approvals (
            id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            run_id        bigint NOT NULL REFERENCES agent_runs (id),
            user_id       bigint NOT NULL REFERENCES users (id),
            hold_id       bigint NOT NULL REFERENCES holds (id),
            amount_paise  integer NOT NULL CHECK (amount_paise > 0),
            status        text NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'approved', 'rejected', 'used')),
            payment_token text,  -- set on approve, cleared the moment it's used
            expires_at    timestamptz NOT NULL,
            decided_at    timestamptz,
            used_at       timestamptz,
            created_at    timestamptz NOT NULL DEFAULT now(),
            CHECK ((payment_token IS NOT NULL) = (status = 'approved'))
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX approvals_one_live_per_hold
        ON approvals (hold_id) WHERE status IN ('pending', 'approved')
    """)
    op.execute(f"""
        CREATE TABLE policy_chunks (
            id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            venue_id  bigint REFERENCES venues (id),  -- NULL: applies to every venue
            doc       text NOT NULL,
            section   text NOT NULL,
            text      text NOT NULL,
            embedding vector({EMBEDDING_DIMENSIONS}) NOT NULL,
            tsv       tsvector GENERATED ALWAYS AS (to_tsvector('english', section || ' ' || text))
                      STORED,
            UNIQUE (doc, section)
        )
    """)
    op.execute(
        "CREATE INDEX policy_chunks_embedding_idx ON policy_chunks "
        "USING hnsw (embedding vector_cosine_ops)"
    )
    op.execute("CREATE INDEX policy_chunks_tsv_idx ON policy_chunks USING gin (tsv)")
    # LangGraph checkpoints live in their own schema. The agent role can reach only that
    # schema, so even a compromised agent worker can't read or write booking tables.
    op.execute("CREATE SCHEMA agent")
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rushline_agent') THEN
                CREATE ROLE rushline_agent NOLOGIN;  -- deploys grant LOGIN and a password
            END IF;
        END $$
    """)
    op.execute("GRANT USAGE, CREATE ON SCHEMA agent TO rushline_agent")
    op.execute("REVOKE ALL ON SCHEMA public FROM rushline_agent")


def downgrade() -> None:
    op.execute("REVOKE ALL ON SCHEMA agent FROM rushline_agent")
    op.execute("DROP SCHEMA agent CASCADE")  # the role is cluster-wide, so it stays
    op.execute("DROP TABLE policy_chunks")
    op.execute("DROP TABLE approvals")
    op.execute("DROP TABLE agent_runs")
    op.execute("DROP TABLE conversations")
