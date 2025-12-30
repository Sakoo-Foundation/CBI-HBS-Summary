# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "bssir",
#     "hbsir",
#     "matplotlib==3.10.8",
#     "polars==1.36.1",
#     "seaborn==0.13.2",
# ]
#
# [tool.uv.sources]
# bssir = { git = "https://github.com/Iran-Open-Data/BSSIR.git" }
# hbsir = { git = "https://github.com/Iran-Open-Data/HBSIR.git" }
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from pathlib import Path

    import polars as pl

    import hbsir
    return Path, hbsir, mo, pl


@app.cell
def _(Path):
    comparison_tables_dir = Path("Data/Comparison_Tables")
    comparison_tables_dir.mkdir(exist_ok=True)
    return (comparison_tables_dir,)


@app.cell
def _(pl):
    def get_cbi_data(table_name: str, column_name: str) -> pl.DataFrame:
        return (
            pl.read_csv(f"Data/Cleaned_Tables/{table_name}.csv")
            .sort("Report_Year", "Year")
            .unique("Year", keep="last")
            .rename({column_name: "CBI"})
            .select("Year", "CBI")
            .sort("Year")
            .cast({"Year": pl.Int16})
        )
    return (get_cbi_data,)


@app.cell
def _(hbsir, pl):
    household_properties_71 = pl.from_pandas(
        hbsir.load_table("Weight", "71-76")
        .pipe(hbsir.add_attribute, "Urban_Rural")
    )
    household_properties_77 = pl.from_pandas(
        hbsir.load_table("Weight", "77-03")
        .pipe(hbsir.add_attribute, "Urban_Rural")
        .pipe(hbsir.add_attribute, "County", aspects="cbi_sample", column_names="CBI_Sample")
    )
    return household_properties_71, household_properties_77


@app.cell
def _(household_properties_71, household_properties_77, pl):
    household_properties = (
        pl.concat(
            [
                household_properties_71,
                household_properties_77,
            ],
            how="diagonal_relaxed",
        )
        .cast(
            {
                "Year": pl.Int16,
                "ID": pl.Int64,
            }
        )
    )
    return (household_properties,)


@app.cell
def _(household_properties, pl):
    def create_sci_values(table: pl.DataFrame) -> pl.DataFrame:

        urban_condition = pl.col("Urban_Rural") == "Urban"
        cbi_sample_condition = urban_condition & pl.col("CBI_Sample")
        return (
            household_properties
            .join(table, on=["Year", "ID"], how="left")
            .group_by("Year")
            .agg(
                pl.col(table.columns[-1]).mul(pl.col("Weight"))
                .sum()
                .truediv(pl.col("Weight").sum())
                .alias("SCI_All"),
                pl.col(table.columns[-1]).mul(pl.col("Weight"))
                .filter(urban_condition).sum()
                .truediv(pl.col("Weight").filter(urban_condition).sum())
                .alias("SCI_Urban"),
                pl.col(table.columns[-1]).mul(pl.col("Weight"))
                .filter(cbi_sample_condition).sum()
                .truediv(pl.col("Weight").filter(cbi_sample_condition).sum())
                .alias("SCI_CBI_Sample"),
            )
            .select(pl.all().replace(0, None))
            .fill_nan(None)
            .sort("Year")
        )
    return (create_sci_values,)


@app.cell
def _(Path, comparison_tables_dir):
    def get_file_path(table: str, column: str) -> Path:
        file_path = comparison_tables_dir / f"{table}/{column}.csv"
        file_path.parent.mkdir(exist_ok=True, parents=True)
        return file_path
    return (get_file_path,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Gross Expenditure
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Gross Expenditure Total
    """)
    return


@app.cell
def _(create_sci_values, get_cbi_data, get_file_path, hbsir, pl):
    gross_income = pl.from_pandas(
        hbsir.load_table("Total_Expenditure", "71-03")
        .groupby(["Year", "ID"], as_index=False)["Gross_Expenditure"].sum()
    )

    (
        pl.concat(
            [
                get_cbi_data("annual_gross_expenditure_by_group_normalized", "Total"),
                create_sci_values(gross_income),
            ],
            how="align",
        )
        .write_csv(get_file_path("annual_gross_expenditure_by_group_normalized", "Total"))
    )
    return (gross_income,)


@app.cell
def _(hbsir, pl):
    dfs = []
    for y in range(1383, 1404):
        dfs.append(
            pl.from_pandas(
                hbsir.load_table("Expenditures", y)
                .pipe(
                    hbsir.add_classification,
                    levels=[1, 2, 3],
                    column_names=["L1", "L2", "L3"]),
            )
        )
    return (dfs,)


@app.cell
def _(dfs, pl):
    expenditures_l1 = (
        pl.concat(dfs, how="vertical_relaxed")
        .group_by("Year", "ID", "L1").agg(pl.col("Gross_Expenditure").sum())
        .drop_nulls()
        .pivot(index=["Year", "ID"], columns="L1", values="Gross_Expenditure")
        .fill_null(0)
        .with_columns(pl.sum_horizontal(pl.all().exclude(["Year", "ID"])).alias("total"))
    )
    return (expenditures_l1,)


@app.cell
def _(create_sci_values, expenditures_l1, gross_income, pl):
    def calculate_expenditure_l1_shares(lable: str) -> pl.DataFrame:
        return (
            create_sci_values(expenditures_l1.select("Year", "ID", lable))
            .join(create_sci_values(gross_income), on="Year", suffix="_total")
            .select(
                "Year",
                *(pl.col(c).truediv(f"{c}_total").mul(100) for c in ["SCI_All", "SCI_Urban", "SCI_CBI_Sample"])
            )
        )
    return (calculate_expenditure_l1_shares,)


@app.cell
def _(create_sci_values, expenditures_l1, get_cbi_data, get_file_path, pl):
    (
        pl.concat(
            [
                get_cbi_data("annual_gross_expenditure_by_group_normalized", "Food_and_Beverages"),
                create_sci_values(expenditures_l1.select("Year", "ID", "food_and_non_alcoholic_beverages")),
            ],
            how="align",
        )
        .write_csv(get_file_path("annual_gross_expenditure_by_group_normalized", "Food_and_Beverages"))
    )
    return


@app.cell
def _(calculate_expenditure_l1_shares, get_cbi_data, get_file_path, pl):
    (
        pl.concat(
            [
                get_cbi_data("annual_gross_expenditure_by_group_normalized_share", "Total"),
                calculate_expenditure_l1_shares("total"),
            ],
            how="align",
        )
        .write_csv(get_file_path("annual_gross_expenditure_by_group_normalized_share", "Total"))
    )
    return


@app.cell
def _(calculate_expenditure_l1_shares, get_cbi_data, get_file_path, pl):
    (
        pl.concat(
            [
                get_cbi_data("annual_gross_expenditure_by_group_normalized_share", "Food_and_Beverages"),
                calculate_expenditure_l1_shares("food_and_non_alcoholic_beverages"),
            ],
            how="align",
        )
        .write_csv(get_file_path("annual_gross_expenditure_by_group_normalized_share", "Food_and_Beverages"))
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Household Size
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Average Household Size
    """)
    return


@app.cell
def _(create_sci_values, get_cbi_data, get_file_path, hbsir, pl):
    household_size = pl.from_pandas(
        hbsir.load_table("Equivalence_Scale", "71-03")
        .loc[:, ["Year", "ID", "Family_Size"]]
    )

    (
        pl.concat(
            [
                get_cbi_data("household_size", "Household_Size_Average"),
                create_sci_values(household_size),
            ],
            how="align",
        )
        .write_csv(get_file_path("household_size", "Household_Size_Average"))
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Member Count
    """)
    return


@app.cell
def _(hbsir, pl):
    member_count = pl.from_pandas(
        hbsir.load_table("members_properties", "71-03")
        .groupby(["Year", "ID"])["Member_Number"].count().clip(1, 10)
        .reset_index()
    )
    return (member_count,)


@app.cell
def _(create_sci_values, get_cbi_data, get_file_path, member_count, pl):
    for i_m in range(1, 11):
        (
            pl.concat(
                [
                    get_cbi_data("household_distribution_by_members", f"Household_Size_{i_m}" + ("_plus" if i_m == 10 else "")),
                    (
                        create_sci_values(member_count.with_columns(pl.col("Member_Number").eq(i_m)))
                        .with_columns(pl.col("^SCI_.*$").mul(100))
                    ),
                ],
                how="align",
            )
            .write_csv(get_file_path(
                "household_distribution_by_members", f"Household_Size_{i_m}" + ("_plus" if i_m == 10 else "")
            ))
        )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Employment Status
    """)
    return


@app.cell
def _(hbsir):
    hbsir.load_table("members_properties", "03")["Activity_Status"].value_counts()
    return


@app.cell
def _(hbsir, pl):
    activity_status_table = (
        pl.from_pandas(hbsir.load_table("members_properties", "71-03"))
        .filter(pl.col("Age").ge(6))
        .with_columns(
            pl.when(pl.col("Activity_Status").is_not_null()).then(pl.col("Activity_Status"))
            .when(pl.col("Is_Student")).then(pl.lit("Student"))
            .when(pl.col("Age").lt(10)).then(pl.lit("Other"))
            .otherwise(pl.lit("Unemployed"))
        )
    )
    return (activity_status_table,)


@app.cell
def _(
    activity_status_table,
    create_sci_values,
    get_cbi_data,
    get_file_path,
    pl,
):
    for a in ["Employed", "Unemployed", "Income_without_Work", "Student", "Housekeeper", "Other"]:
        (
            pl.concat(
                [
                    get_cbi_data("employment_status_6_plus", a),
                    (
                        create_sci_values(activity_status_table.with_columns(pl.col("Activity_Status").eq(a)))
                        .with_columns(pl.col("^SCI_.*$").mul(100))
                    ),
                ],
                how="align",
            )
            .write_csv(get_file_path("employment_status_6_plus", a))
        )
    return


@app.cell
def _(activity_status_table, pl):
    employed_count = (
        activity_status_table
        .group_by("Year", "ID").agg(pl.col("Activity_Status").eq("Employed").sum().alias("Employed_Count"))
    )
    return (employed_count,)


@app.cell
def _(create_sci_values, employed_count, get_cbi_data, get_file_path, pl):
    for i_e in range(0, 4):
        (
            pl.concat(
                [
                    get_cbi_data(
                        "household_distribution_by_number_of_employed",
                        f"Employed_{i_e}" + ("_plus" if i_e == 3 else "")
                    ),
                    (
                        create_sci_values(employed_count.with_columns(pl.col("Employed_Count").eq(i_e)))
                        .with_columns(pl.col("^SCI_.*$").mul(100))
                    ),
                ],
                how="align",
            )
            .write_csv(get_file_path(
                "household_distribution_by_number_of_employed", f"Employed_{i_e}" + ("_plus" if i_e == 3 else "")
            ))
        )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Household Appliances
    """)
    return


@app.cell
def _(hbsir, pl):
    def get_house_specification_column_ratio(column: str | pl.Expr) -> pl.DataFrame:
        if isinstance(column, str):
            column = pl.col(column)
        _columns = column.meta.root_names()
        return (
            pl.from_pandas(
                hbsir.load_table("house_specifications", "71-03")
                .loc[:, ["Year", "ID"] + _columns]
            )
            .with_columns(column.mul(100))
        )
    return (get_house_specification_column_ratio,)


@app.cell
def _(
    create_sci_values,
    get_cbi_data,
    get_file_path,
    get_house_specification_column_ratio,
    pl,
):
    def create_house_specification_comparison_table(column: str | pl.Expr, table_name: str) -> None:
        if isinstance(column, str):
            column = pl.col(column)
        (
            pl.concat(
                [
                    get_cbi_data(table_name, column.meta.output_name()),
                    create_sci_values(get_house_specification_column_ratio(column)),
                ],
                how="align",
            )
            .write_csv(get_file_path(table_name, column.meta.output_name()))
        )
    return (create_house_specification_comparison_table,)


@app.cell
def _(create_sci_values, get_house_specification_column_ratio):
    create_sci_values(get_house_specification_column_ratio("Car"))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Car
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Car", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Motorcycle
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Motorcycle", "household_appliances_access")
    return


@app.cell
def _(hbsir):
    hbsir.load_table("house_specifications", "3").columns
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Bicycle
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Bicycle", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Sewing Machine
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Sewing_Machine", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Radio
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Radio", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### TV
    """)
    return


@app.cell
def _(create_house_specification_comparison_table, pl):
    create_house_specification_comparison_table(
        pl.col("Color_TV").or_(pl.col("Black_and_White_TV")).alias("TV"),
        "household_appliances_access",
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### VCR
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("VCR", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Refrigerator
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Refrigerator", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Freezer and Freezer-Refrigerator
    """)
    return


@app.cell
def _(create_house_specification_comparison_table, pl):
    create_house_specification_comparison_table(
        pl.col("Freezer").or_(pl.col("Freezer_Refrigerator")).alias("Freezer_and_Freezer_Refrigerator"),
        "household_appliances_access",
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Freezer and Refrigerator
    """)
    return


@app.cell
def _(create_house_specification_comparison_table, pl):
    create_house_specification_comparison_table(
        pl.col("Freezer")
        .or_(pl.col("Refrigerator"))
        .or_(pl.col("Freezer_Refrigerator"))
        .alias("Refrigerator_Freezer_and_Freezer_Refrigerator"),
        "household_appliances_access",
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Oven
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Oven", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### PC
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("PC", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Fan
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Fan", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Vacuum Cleaner
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Vacuum_Cleaner", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Washing Machine
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Washing_Machine", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Cellphone
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Cellphone", "household_appliances_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Pipe Water
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Pipe_Water", "household_facilities_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Electricity
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Electricity", "household_facilities_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Natural_Gas
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Natural_Gas", "household_facilities_access")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Sewerage
    """)
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Sewerage", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Phone", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Kitchen", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Bathroom", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Central_Heating", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table):
    create_house_specification_comparison_table("Internet", "household_facilities_access")
    return


@app.cell
def _(create_house_specification_comparison_table, pl):
    for room_count in range(1, 6):
        create_house_specification_comparison_table(
            pl.col("Number_of_Rooms").eq(room_count).alias(f"Dwelling_Room_{room_count}"),
            "housing_rooms",
        )
    create_house_specification_comparison_table(
        pl.col("Number_of_Rooms").ge(6).alias(f"Dwelling_Room_6_Plus"),
        "housing_rooms",
    )
    return


@app.cell
def _(hbsir):
    (
        hbsir.load_table("house_specifications", "71-3")
        .pipe(hbsir.add_weight)
        .groupby(["Year", "Number_of_Rooms"])["Weight"].sum()
        .unstack()
        .drop(columns=[0])
        .rename(lambda n: str(n), axis="columns")
        .assign(**{"6+": (lambda df: df[[c for c in df.columns if int(c) >= 6]].sum(axis="columns"))})
        .loc[:, [str(c) for c in range(1, 6)] + ["6+"]]
        .pipe(lambda df: df.div(df.sum(axis="columns"), axis="index"))
        .mul(100)
    )
    return


@app.cell
def _():
    columns = {
        "annual_gross_expenditure_by_group": [
            "Gross_Expenditure_Total",
        ],
        "annual_gross_expenditure_by_group_normalized": [
            "Total",
            "Food_and_Beverages",
        ],
        "annual_gross_expenditure_by_group_normalized_share": [
            "Total",
            "Food_and_Beverages",
        ],
        "household_size": [
            "Household_Size",
        ],
        "household_distribution_by_members": [
            "Household_Size_1",
            "Household_Size_2",
            "Household_Size_3",
            "Household_Size_4",
            "Household_Size_5",
            "Household_Size_6",
            "Household_Size_7",
            "Household_Size_8",
            "Household_Size_9",
            "Household_Size_10_plus",
        ],
        "employment_status_6_plus": [
            "Employed",
            "Unemployed",
            "Unemployed",
            "Income_without_Work",
            "Student",
            "Housekeeper",
            "Other",
        ],
        "household_distribution_by_number_of_employed": [
            "Employed_0",
            "Employed_1",
            "Employed_2",
            "Employed_3_plus",
        ],
        "housing_rooms": [
            "Dwelling_1_Room",
            "Dwelling_2_Room",
            "Dwelling_3_Room",
            "Dwelling_4_Room",
            "Dwelling_5_Room",
            "Dwelling_6_Room_Plus",
        ],
        "household_appliances_access": [
            "Car",
            "Motorcycle",
            "Bicycle",
            "Sewing_Machine",
            # "Radio_and_Cassette",
            # "Radio",
            "TV",
            "VCR",
            "PC",
            "Refrigerator",
            "Freezer_and_Freezer_Refrigerator",
            "Refrigerator_Freezer_and_Freezer_Refrigerator",
            "Fan",
            "Oven",
            "Vacuum_Cleaner",
            "Washing_Machine",
            "Cellphone",
            # "Camera",
        ],
        "household_facilities_access": [
            "Pipe_Water",
            "Electricity",
            "Natural_Gas",
            "Sewerage",
            "Phone",
            "Kitchen",
            "Bathroom",
            "AC",
            "Central_Heating",
            "Storage_Room",
            "Internet",
            "Social_Media_Access",
        ],
    }
    return (columns,)


@app.cell
def _(columns, mo):
    table = mo.ui.dropdown(list(columns.keys()), "household_appliances_access", label="Table")
    return (table,)


@app.cell
def _(columns, mo, table):
    column = mo.ui.dropdown(columns[table.value], columns[table.value][0], label="Column")
    return (column,)


@app.cell
def _(mo):
    include_zero = mo.ui.checkbox(value=False, label="Include zero on Y-axis")
    return (include_zero,)


@app.cell
def _(mo):
    log_scale = mo.ui.checkbox(value=False, label="Log scale Y-axis")
    return (log_scale,)


@app.cell
def _():
    title_mapping = {
        ("household_appliances_access", "Car"): "Car Ownership",
        ("household_appliances_access", "Motorcycle"): "Motorcycle Ownership",
        ("household_appliances_access", "Bicycle"): "Bicycle Ownership",
        ("household_appliances_access", "Radio"): "Radio Ownership",
        ("household_appliances_access", "Refrigerator"): "Refrigerator Ownership",
        ("household_appliances_access", "Freezer_and_Freezer_Refrigerator"): "Freezer and Freezer-Refrigerator Ownership",
        ("household_appliances_access", "Refrigerator_Freezer_and_Freezer_Refrigerator"): "Refrigerator and Freezer Ownership",
        ("household_appliances_access", "Vacuum_Cleaner"): "Vacuum Cleaner Ownership",
        ("household_appliances_access", "Washing_Machine"): "Washing Machine Ownership",
        ("household_appliances_access", "Cellphone"): "Cellphone Ownership",
        ("household_facilities_access", "Pipe_Water"): "Pipe_Water Access",
        ("household_facilities_access", "Electricity"): "Electricity Access",
        ("household_facilities_access", "Natural_Gas"): "Natural Gas Access",
        ("household_facilities_access", "Phone"): "Phone Access",
        ("household_facilities_access", "Kitchen"): "Kitchen Access",
        ("household_facilities_access", "Bathroom"): "Bathroom Access",
        ("household_facilities_access", "Sewerage"): "Sewerage Access",
    }
    return (title_mapping,)


@app.cell
def _(column, fig, include_zero, log_scale, mo, table):
    mo.vstack(
        [
            mo.hstack([table, column], justify="start"),
            mo.hstack([include_zero, log_scale], justify="start"),
            fig,
        ]
    )
    return


@app.cell
def _(
    column,
    get_file_path,
    include_zero,
    log_scale,
    pl,
    table,
    title_mapping,
):
    import matplotlib.pyplot as plt
    import seaborn as sns

    title = title_mapping.get((table.value, column.value), column.value)

    plot_table = (
        pl.read_csv(get_file_path(table.value, column.value))
        .unpivot(index="Year", variable_name="Source", value_name="Value")
        .with_columns(pl.col("Source").str.replace_all("_", " "))
        .cast({"Source": pl.Enum(["SCI All", "SCI Urban", "SCI CBI Sample", "CBI"])})
        .to_pandas()
    )

    if table.value == "household_size":
        ylabel = "Average Household Size"
    elif table.value in [
        "household_appliances_access",
        "household_distribution_by_members",
        "household_distribution_by_number_of_employed",
    ]:
        ylabel = "Percentage of Households (%)"
    elif "6_plus" in table.value:
        ylabel = "Percentage of Individuals (%)"
    elif "share" in table.value:
        ylabel = "Percentage of Expenditures (%)"
    else:
        ylabel = "Thousand Tomans"
        plot_table["Value"] = plot_table["Value"].div(1e4)

    sns.set_theme()
    fig, ax = plt.subplots(figsize=(10, 5))

    ax = sns.lineplot(
        plot_table,
        x="Year",
        y="Value",
        hue="Source",
        palette=["#94B4C1", "#547792", "#213448", "#FA6868"],
        marker="o",
    )

    ax.set_title(f"Comparative Analysis of {title} (SCI vs. CBI)", fontsize=18)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)

    if include_zero.value:
        ax.set_ylim(0, ax.get_ylim()[-1])

    if log_scale.value:
        ax.set_yscale("log")
    else:
        ax.set_yscale("linear")
    return (fig,)


if __name__ == "__main__":
    app.run()
