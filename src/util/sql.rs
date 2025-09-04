use anyhow::Result;
use sqlx::{postgres::PgArguments, Postgres, PgPool};
use sqlx::query::Query;

// Generic paged execution loop for DELETEs (or any query that returns rows_affected).
// The `build` closure should produce a query with a LIMIT placeholder bound last.
pub async fn paged_loop<F, C>(pool: &PgPool, mut build: F, batch: i64, mut on_batch: C) -> Result<()>
where
    F: FnMut(i64) -> Query<'static, Postgres, PgArguments>,
    C: FnMut(u64),
{
    loop {
        let res = build(batch).execute(pool).await?;
        let n = res.rows_affected();
        if n == 0 { break; }
        on_batch(n);
    }
    Ok(())
}
