import pandas as pd
import plotly.express as px
import streamlit as st


RATING_LABEL_MAP = {
    "rating_1_count": "⭐1",
    "rating_2_count": "⭐2",
    "rating_3_count": "⭐3",
    "rating_4_count": "⭐4",
    "rating_5_count": "⭐5",
    "no_rating_count": "No rating",
}


def prepare_stacked_data(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
    order_by: str | None = None,
    top_n: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_plot = df.copy()

    if order_by and order_by in df_plot.columns:
        df_plot = df_plot.sort_values(order_by, ascending=False)

    if top_n:
        df_plot = df_plot.head(top_n)

    df_long = df_plot.melt(
        id_vars=[x_col],
        value_vars=y_cols,
        var_name="rating",
        value_name="count",
    )

    df_long["rating"] = df_long["rating"].replace(RATING_LABEL_MAP)

    return df_plot, df_long


def render_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str | None = None,
):
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        markers=True,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_stack_bar(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
    order_by: str | None = None,
):
    top_n = st.slider("Top categories", 5, min(60, len(df)), min(20, len(df)))

    df_plot, df_long = prepare_stacked_data(
        df=df,
        x_col=x_col,
        y_cols=y_cols,
        order_by=order_by,
        top_n=top_n,
    )

    fig = px.bar(
        df_long,
        x=x_col,
        y="count",
        color="rating",
        barmode="stack",
        category_orders={x_col: df_plot[x_col].tolist()},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_stack_bar_percent(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
    order_by: str | None = None,
):
    top_n = st.slider("Top categories", 5, min(60, len(df)), min(20, len(df)))

    df_plot, df_long = prepare_stacked_data(
        df=df,
        x_col=x_col,
        y_cols=y_cols,
        order_by=order_by,
        top_n=top_n,
    )

    totals = df_long.groupby(x_col)["count"].transform("sum")
    df_long["percent"] = df_long["count"] / totals * 100

    fig = px.bar(
        df_long,
        x=x_col,
        y="percent",
        color="rating",
        barmode="stack",
        category_orders={x_col: df_plot[x_col].tolist()},
    )
    fig.update_yaxes(title="Percent")
    st.plotly_chart(fig, use_container_width=True)


def render_box_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str | None = None,
):
    fig = px.box(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_heatmap(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    z_col: str,
):
    pivot_df = df.pivot(index=y_col, columns=x_col, values=z_col)

    fig = px.imshow(
        pivot_df,
        aspect="auto",
        labels={"x": x_col, "y": y_col, "color": z_col},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str | None = None,
    order_by: str | None = None,
):
    df_plot = df.copy()

    if order_by and order_by in df_plot.columns:
        df_plot = df_plot.sort_values(order_by, ascending=False)

    top_n = st.slider("Top categories", 5, min(60, len(df_plot)), min(20, len(df_plot)))
    df_plot = df_plot.head(top_n)

    fig = px.bar(
        df_plot,
        x=x_col,
        y=y_col,
        color=color_col,
        category_orders={x_col: df_plot[x_col].tolist()},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_chart(chart_item: dict, df: pd.DataFrame):
    chart_type = chart_item.get("chart_type", "line")
    x_col = chart_item.get("x")
    y_col = chart_item.get("y")
    y_cols = chart_item.get("y_cols")
    color_col = chart_item.get("color")
    z_col = chart_item.get("z")
    order_by = chart_item.get("order_by")

    if chart_type == "line":
        render_line_chart(df, x_col=x_col, y_col=y_col, color_col=color_col)

    elif chart_type == "stack_bar":
        render_stack_bar(df, x_col=x_col, y_cols=y_cols, order_by=order_by)

    elif chart_type == "stack_bar_percent":
        render_stack_bar_percent(df, x_col=x_col, y_cols=y_cols, order_by=order_by)

    elif chart_type == "box":
        render_box_chart(df, x_col=x_col, y_col=y_col, color_col=color_col)

    elif chart_type == "heatmap":
        render_heatmap(df, x_col=x_col, y_col=y_col, z_col=z_col)

    elif chart_type == "bar":
      render_bar_chart(
        df,
        x_col=x_col,
        y_col=y_col,
        color_col=color_col,
        order_by=order_by,
    )

    else:
        st.warning(f"Unsupported chart_type: {chart_type}")