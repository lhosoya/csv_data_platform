# Take-Home Challenge: Data Engineer

## **Objective**

Design a scalable and reliable data platform architecture for a SaaS application. The goal is to evaluate your ability to think strategically about data engineering at scale, considering ingestion, storage, transformation, and analytics needs.

---

## Resources

- Assume the SaaS application generates **1M+ user interaction events per day** (e.g., app events, subscriptions).
- The company has a **small data team** and limited infrastructure budget, so **simplicity and maintainability** matter.
- You may use any tools, technologies, or frameworks you’re most comfortable with (cloud-native or open-source).
- We will provide a **CSV file** with raw event data for you to use in your solution.
    
    [sample_events.csv](attachment:182f126b-04b1-45c9-b8f4-93bb0c7b9987:sample_events.csv)
    
    - `event_id` (string, unique if present)
    - `user_id` (string/int)
    - `event_type` (enum: `signup|trial_started|subscription_started|subscription_canceled`)
        > Can have more enums:
        >> signoff/signout -> account "deleted"
        >> trial ended
        >> trial canceled
    - `event_ts` (ISO timestamp)
    - `plan_id` (nullable)
    - `amount` (nullable)
        > Plan_id is related to amount
        >> A way to improve over the time whether the amount is higher, could get less subscriptions/cancelled.
    - `source` (utm/source, nullable).

** if a column you need is missing, state your **assumption** and proceed.*

---

## **Your Task**

1. **Architecture (high-level)**
    
    Provide a diagram and 3–6 bullets per layer showing **how data flows** from source → ingestion → storage → transform → serving.
    
    - Call out **batch vs. streaming** (and why).
    - Mention **orchestration**, **lineage**, **observability**, and **cost guards** (e.g., partitioning, file formats).
    > ZSTD, ZOrder Timestamp
2. **Modeling the Data**
    
    Using the dataset provided (`sample_events.csv`):
    
    - **2.1 Deduplication model**
        - Remove duplicates; state **how** you identify a canonical record (e.g., `(event_id)` or `(user_id, event_type, ts)` + tie-breaker).
        - Show a small **SQL** or transformation snippet.
    - **2.2 Analytics model** (dashboard-ready)
        
        Build a model or view that can answer:
        
        - **Active Subscribers** (point-in-time and over time)
        - **Conversion funnel** (lead → sign-up → trial → paid)
        - **Retention / churn** (monthly)
            
            Provide example **queries** (or Metabase questions) that read from your model.
            
3. **Data Quality & Observability**
    
    Briefly define:
    
    - **Tests** you’d enforce (e.g., not null, uniqueness, referential checks, accepted values).
    > Related to user having the same subscription within a period of the same product.
    - **SLIs/SLOs** (e.g., data freshness, pipeline success rate).
    > Always up-to-date data, what is the tolerance level for this kind of data.
    - **Alerting** (what triggers, who gets pinged, where).
    > Alerts dashboard, add a grafana? Alert late event, past a SLA; Alert resub?
4. **Scalability & Tradeoffs**
    - How you’d evolve from 1M/day → **10M/day**:
        - Storage format/partitioning, compute patterns, concurrency limits.
    - Make explicit **trade-offs** (latency vs. cost, batch vs. stream, dbt vs. custom, etc.).
5. **Full Load vs. Incremental Loads**
    - Design for **full reloads** and **incremental/delta** updates.
    - How you **detect and reconcile** late/corrected events (e.g., watermarking, upserts/MERGE, SCD types, idempotency keys).
    > With SLAs
6. **SQL Challenge – Continuous Subscription Periods**
    
    Using the dataset provided (`sample_events.csv`):
    
    - You have events like:
        - `signup`
        - `trial_started`
        - `subscription_started`
        - `subscription_canceled`
    
    Each `subscription_started` followed by an optional `subscription_canceled` defines a **subscription period** for a user.
    
    Some users may **resubscribe** after canceling, creating multiple periods.
    
    Write a SQL query that returns each user’s **continuous subscription periods**, merging **overlapping or consecutive ranges** into a single period.
    
    - Input: the raw event log (from the CSV).
    - Output: a table with columns like:
    
    ```sql
    user_id,
    period_start_date,
    period_end_date
    ```
    
    where:
    
    - `period_start_date` = timestamp of the first `subscription_started` in a continuous streak.
    - `period_end_date` = timestamp of the matching `subscription_canceled` (or `NULL` if still active).
    - If a user cancels and later resubscribes, that should create a **new period.**
    
    Example:
    
    - **User A**
        - `2025-01-01 → 2025-01-15` (first subscription, then canceled)
        - `2025-01-29 → NULL` (resubscribed, still active)
    - **User B**
        - `2025-01-05 → 2025-01-20`
        - `2025-02-03 → 2025-02-04`
    
7. **Deliverables**
    - **README (2–3 pages)**: architecture rationale, trade-offs, and ops plan.
    - **Diagram** (image, slide, or diagram tool link).
    - **SQL / dbt / notebooks** (minimal but runnable).
    - **Example queries** for the dashboard questions.
    - **SQL solution** for gaps & islands.

---

## **Submission Requirements**

1. **Confirm Your Timeline**:
    - Reply this email with your expected completion timeline.
2. **Submit Your Work**:
    - Submit your deliverables.
    - Submit **at least 24 hours before your follow-up discussion** for review.

---

We look forward to your submission! 

If you have any questions or need clarification, feel free to reach out. Good luck!