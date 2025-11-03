"""Create documents table with vector support for RAG system

Revision ID: 0001
Revises:
Create Date: 2024-08-04 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # Create documents table
    op.create_table(
        "documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("embedding", sa.String(), nullable=True),
        sa.Column("filename", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    # Create vector column for embeddings (1536 dimensions for OpenAI text-embedding-3-small)
    op.execute("ALTER TABLE documents ADD COLUMN embedding_vector vector(1536)")

    # Create index for similarity search
    op.execute(
        "CREATE INDEX IF NOT EXISTS documents_embedding_idx ON documents USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100)"
    )

    # Create function for similarity search
    op.execute(
        """
        CREATE OR REPLACE FUNCTION similarity_search(
            query_embedding vector(1536),
            match_threshold float,
            match_count int
        )
        RETURNS TABLE (
            id integer,
            content text,
            metadata_json json,
            filename text,
            similarity float
        )
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN QUERY
            SELECT
                documents.id,
                documents.content,
                documents.metadata_json,
                documents.filename,
                1 - (documents.embedding_vector <=> query_embedding) AS similarity
            FROM documents
            WHERE 1 - (documents.embedding_vector <=> query_embedding) > match_threshold
            ORDER BY documents.embedding_vector <=> query_embedding
            LIMIT match_count;
        END;
        $$;
    """
    )

    # Create trigger to update updated_at timestamp
    op.execute(
        """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """
    )

    op.execute(
        """
        CREATE TRIGGER update_documents_updated_at
        BEFORE UPDATE ON documents
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )


def downgrade() -> None:
    # Drop trigger
    op.execute("DROP TRIGGER IF EXISTS update_documents_updated_at ON documents")

    # Drop function
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")

    # Drop similarity search function
    op.execute("DROP FUNCTION IF EXISTS similarity_search(vector, float, int)")

    # Drop table
    op.drop_table("documents")

    # Drop extension (optional - might be used by other tables)
    # op.execute('DROP EXTENSION IF EXISTS vector')
