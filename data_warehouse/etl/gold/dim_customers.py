import sys
from pathlib import Path
import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

# from transform_transactiions_data import transform_transactions_data
from transform_customers_data import transform_customers_data

# Load customers
customers_df = transform_customers_data()

# Create dimension table for customer
dim_customer = pd.DataFrame(columns=[
    "customer_key",
    "customer_id",
    # "email",
    "country",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
])

# SCD Type 2 upsert function for customer dimension
def scd2_upsert_customer(dim_customer: pd.DataFrame,
                         stg_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Perform SCD Type 2 upsert for customer dimension.
    """

    today = pd.Timestamp.today().normalize()

    # Ensure staging has unique business keys
    stg_customers = stg_customers.drop_duplicates(subset=["customer_id"])

    # ------------------------------------------------------------------
    # CASE 1: Empty dimension → insert all staging rows
    # ------------------------------------------------------------------
    if dim_customer.empty:
        inserts = stg_customers.copy()

        inserts["customer_key"] = range(1, len(inserts) + 1)
        inserts["effective_from"] = today
        inserts["effective_to"] = pd.NaT
        inserts["is_current"] = True

        result = inserts[[
            "customer_key",
            "customer_id",
            # "email",
            "country",
            "signup_date",
            "effective_from",
            "effective_to",
            "is_current"
        ]]

        # ------------------ TESTS ------------------
        assert result["customer_key"].is_unique, "Customer keys are not unique"
        assert result["is_current"].all(), "Initial rows must be current"
        assert result["effective_to"].isna().all(), "Initial rows must be open-ended"
        # -------------------------------------------

        return result

    # ------------------------------------------------------------------
    # CASE 2: Incremental SCD2 logic
    # ------------------------------------------------------------------
    dim_current = dim_customer[dim_customer["is_current"]].copy()

    merged = stg_customers.merge(
        dim_current,
        on="customer_id",
        how="left",
        suffixes=("_stg", "_dim")
    )

    # Identify new customers
    is_new = merged["customer_key"].isna()

    # NULL-safe change detection
    # email_changed = (
    #     merged["email_stg"].fillna("") != merged["email_dim"].fillna("")
    # )

    country_changed = (
        merged["country_stg"].fillna("") != merged["country_dim"].fillna("")
    )

    is_changed = ~is_new & country_changed
    # is_changed = ~is_new & (email_changed | country_changed)

    # ------------------------------------------------------------------
    # Expire existing records
    # ------------------------------------------------------------------
    keys_to_expire = merged.loc[is_changed, "customer_key"]

    if not keys_to_expire.empty:
        dim_customer.loc[
            dim_customer["customer_key"].isin(keys_to_expire),
            ["effective_to", "is_current"]
        ] = [today, False]

        # ------------------ TESTS ------------------
        expired = dim_customer.loc[
            dim_customer["customer_key"].isin(keys_to_expire)
        ]
        assert not expired["is_current"].any(), "Expired rows still marked current"
        assert expired["effective_to"].notna().all(), "Expired rows missing end date"
        # -------------------------------------------

    # ------------------------------------------------------------------
    # Prepare inserts (new + changed)
    # ------------------------------------------------------------------
    inserts = merged.loc[is_new | is_changed, [
        "customer_id",
        # "email_stg",
        "country_stg",
        "signup_date_stg"
    ]].rename(columns={
        # "email_stg": "email",
        "country_stg": "country",
        "signup_date_stg": "signup_date"
    })
    assert "signup_date_stg" in merged.columns, "signup_date missing after merge"

    if inserts.empty:
        return dim_customer

    next_key = dim_customer["customer_key"].max() + 1

    inserts["customer_key"] = range(next_key, next_key + len(inserts))
    inserts["effective_from"] = today
    inserts["effective_to"] = pd.NaT
    inserts["is_current"] = True

    dim_customer = pd.concat([dim_customer, inserts], ignore_index=True)

    # ------------------ TESTS ------------------
    current_rows = dim_customer[dim_customer["is_current"]]

    assert current_rows["customer_id"].is_unique, \
        "More than one current row per customer detected"

    assert dim_customer["customer_key"].is_unique, \
        "Customer key uniqueness violated"

    assert dim_customer.loc[
        dim_customer["is_current"], "effective_to"
    ].isna().all(), "Current rows must have NULL effective_to"
    # -------------------------------------------

    # Save dimension table to CSV
 

    return dim_customer

# Output path for the dimension table
output_path = (
    Path(__file__).resolve().parent.parent.parent
    / "processed_data"
    / "dim_customers.csv"
)
dim_customer = scd2_upsert_customer(dim_customer, customers_df)
dim_customer.to_csv(output_path, index=False)
# print(dim_customer.sort_values(
#     ["customer_id", "effective_from"]
# ))

# Initial load test
# try:
#     dim_customer = scd2_upsert_customer(dim_customer, customers_df)
#     print("==>> SCD Type 2 tests passed")
#     print(dim_customer.sort_values(
#     ["customer_id", "effective_from"]
# ))

# except AssertionError as e:
#     print("==> SCD Type 2 test failed:")
#     print(e)