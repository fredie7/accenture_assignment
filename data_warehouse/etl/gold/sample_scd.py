# Create dimension table for customer
dim_customer = pd.DataFrame(columns=[
    "customer_key",
    "customer_id",
    "email",
    "country",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
])

def scd2_upsert_customer(dim_customer, stg_customers):
    today = pd.Timestamp.today().normalize()

    # -------------------------
    # CASE 1: Empty dimension → all rows are new
    # -------------------------
    if dim_customer.empty:
        inserts = stg_customers.copy()

        inserts = inserts.assign(
            customer_key=range(1, len(inserts) + 1),
            effective_from=today,
            effective_to=pd.NaT,
            is_current=True
        )

        return inserts[[
            "customer_key",
            "customer_id",
            "email",
            "country",
            "signup_date",
            "effective_from",
            "effective_to",
            "is_current"
        ]]

    # -------------------------
    # CASE 2: Normal SCD2 logic
    # -------------------------
    dim_current = dim_customer[dim_customer["is_current"]].copy()

    merged = stg_customers.merge(
        dim_current,
        on="customer_id",
        how="left",
        suffixes=("_stg", "_dim")
    )

    is_new = merged["customer_key"].isna()

    is_changed = (
        ~is_new &
        (
            (merged["email_stg"] != merged["email_dim"]) |
            (merged["country_stg"] != merged["country_dim"])
        )
    )

    # Expire changed records
    dim_customer.loc[
        dim_customer["customer_key"].isin(merged.loc[is_changed, "customer_key"]),
        ["effective_to", "is_current"]
    ] = [today, False]

    # Prepare inserts
    inserts = merged.loc[is_new | is_changed, [
        "customer_id",
        "email_stg",
        "country_stg",
        "signup_date"
    ]].rename(columns={
        "email_stg": "email",
        "country_stg": "country"
    })

    if not inserts.empty:
        next_key = dim_customer["customer_key"].max() + 1

        inserts = inserts.assign(
            customer_key=range(next_key, next_key + len(inserts)),
            effective_from=today,
            effective_to=pd.NaT,
            is_current=True
        )

        dim_customer = pd.concat([dim_customer, inserts], ignore_index=True)

    return dim_customer


dim_customer = scd2_upsert_customer(dim_customer, customers_df)