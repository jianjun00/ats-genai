from typing import List, Optional
from domains.market_data.services.agent.models import ReconciledRecord
from shared.utils.environment import Environment
import asyncpg
import json

class ReconciledRecordDAO:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.env = Environment()
        self.table = self.env.get_table_name("reconciled_records")

    async def insert(self, record: ReconciledRecord) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {self.table}
                (instrument_id, as_of, data_type, value, quality_score, sources, rationale, provenance)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (instrument_id, as_of, data_type) DO UPDATE
                SET value = EXCLUDED.value,
                    quality_score = EXCLUDED.quality_score,
                    sources = EXCLUDED.sources,
                    rationale = EXCLUDED.rationale,
                    provenance = EXCLUDED.provenance,
                    updated_at = NOW()
            ''',
                record.instrument_id,
                record.as_of,
                record.data_type,
                json.dumps(record.value),
                record.quality_score,
                record.sources,
                record.rationale,
                json.dumps(record.provenance)
            )

    async def get(self, instrument_id: str, as_of, data_type: str) -> Optional[ReconciledRecord]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f'''
                SELECT instrument_id, as_of, data_type, value, quality_score, sources, rationale, provenance
                FROM {self.table}
                WHERE instrument_id = $1 AND as_of = $2 AND data_type = $3
            ''', instrument_id, as_of, data_type)
            if not row:
                return None
            value = row["value"]
            if isinstance(value, str):
                value = json.loads(value)
            provenance = row["provenance"]
            if isinstance(provenance, str):
                provenance = json.loads(provenance)
            return ReconciledRecord(
                instrument_id=row["instrument_id"],
                as_of=row["as_of"],
                data_type=row["data_type"],
                value=value,
                quality_score=row["quality_score"],
                sources=row["sources"],
                rationale=row["rationale"],
                provenance=provenance
            )

    async def list_for_instrument(self, instrument_id: str, data_type: str = "eod") -> List[ReconciledRecord]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f'''
                SELECT instrument_id, as_of, data_type, value, quality_score, sources, rationale, provenance
                FROM {self.table}
                WHERE instrument_id = $1 AND data_type = $2
                ORDER BY as_of DESC
            ''', instrument_id, data_type)
            return [
                ReconciledRecord(
                    instrument_id=row["instrument_id"],
                    as_of=row["as_of"],
                    data_type=row["data_type"],
                    value=row["value"],
                    quality_score=row["quality_score"],
                    sources=row["sources"],
                    rationale=row["rationale"],
                    provenance=row["provenance"]
                ) for row in rows
            ]
