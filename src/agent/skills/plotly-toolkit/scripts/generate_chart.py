#!/usr/bin/env python3
"""
generate_chart.py - Plotly-based Chart Generation
==================================================
Automatic chart generation using Plotly Express with smart chart type selection.

Usage:
    # Auto-recommend chart type
    python scripts/generate_chart.py --data results.csv --auto --output artifacts/charts/revenue.html

    # Specify chart type
    python scripts/generate_chart.py --data results.csv --chart-type bar \
        --x-column category --y-column revenue --output artifacts/charts/revenue.html

    # Line chart for time series
    python scripts/generate_chart.py --data results.csv --chart-type line \
        --x-column date --y-column value --title "Trend" --output artifacts/charts/trend.html
"""

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

# =============================================================================
# Chart Type Selection
# =============================================================================

class ChartTypeSelector:
    """Standard chart type selection based on data characteristics."""

    @staticmethod
    def analyze_data(df: pd.DataFrame) -> dict[str, Any]:
        """Analyze DataFrame characteristics."""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime64[ns]', 'period']).columns.tolist()

        return {
            'row_count': len(df),
            'column_count': len(df.columns),
            'numeric_cols': numeric_cols,
            'categorical_cols': categorical_cols,
            'datetime_cols': datetime_cols,
            'has_time': len(datetime_cols) > 0,
        }

    @staticmethod
    def recommend_chart(df: pd.DataFrame, x_column: str | None = None,
                       y_column: str | None = None) -> str:
        """
        Recommend chart type using standard logic.

        Returns: bar, line, scatter, pie, histogram, box
        """
        analysis = ChartTypeSelector.analyze_data(df)

        # User specified both columns
        if x_column and y_column:
            x_is_numeric = pd.api.types.is_numeric_dtype(df[x_column])
            y_is_numeric = pd.api.types.is_numeric_dtype(df[y_column])

            if x_is_numeric and y_is_numeric:
                return 'scatter'
            elif not x_is_numeric and y_is_numeric:
                return 'bar'
            else:
                return 'bar'

        # Time series data
        if analysis['has_time'] and len(analysis['numeric_cols']) > 0:
            return 'line'

        # Single numeric column → distribution
        if len(analysis['numeric_cols']) == 1 and len(analysis['categorical_cols']) == 0:
            return 'histogram'

        # Categorical + numeric → comparison
        if len(analysis['categorical_cols']) >= 1 and len(analysis['numeric_cols']) >= 1:
            # Check if it's a part-to-whole scenario (few categories)
            unique_values = df[analysis['categorical_cols'][0]].nunique()
            if unique_values <= 8 and len(analysis['numeric_cols']) == 1:
                return 'pie'
            return 'bar'

        # Multiple numeric → correlation
        if len(analysis['numeric_cols']) >= 2:
            return 'scatter'

        # Categorical only → count
        if len(analysis['categorical_cols']) >= 1:
            return 'bar'

        # Default
        return 'bar'

    @staticmethod
    def suggest_columns(df: pd.DataFrame, chart_type: str) -> tuple[str | None, str | None]:
        """Suggest x and y columns for the chart type."""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns.tolist()

        if chart_type == 'line':
            x = datetime_cols[0] if datetime_cols else (categorical_cols[0] if categorical_cols else df.columns[0])
            y = numeric_cols[0] if numeric_cols else None
            return x, y

        elif chart_type in ['bar', 'area']:
            x = categorical_cols[0] if categorical_cols else df.columns[0]
            y = numeric_cols[0] if numeric_cols else None
            return x, y

        elif chart_type == 'scatter':
            if len(numeric_cols) >= 2:
                return numeric_cols[0], numeric_cols[1]
            return numeric_cols[0] if numeric_cols else df.columns[0], None

        elif chart_type == 'pie':
            x = categorical_cols[0] if categorical_cols else df.columns[0]
            y = numeric_cols[0] if numeric_cols else None
            return x, y

        elif chart_type == 'histogram':
            x = numeric_cols[0] if numeric_cols else df.columns[0]
            return x, None

        elif chart_type == 'box':
            y = numeric_cols[0] if numeric_cols else df.columns[0]
            x = categorical_cols[0] if categorical_cols else None
            return x, y

        return df.columns[0] if len(df.columns) > 0 else None, None


# =============================================================================
# Plotly Chart Generator (Standard)
# =============================================================================

class PlotlyChartGenerator:
    """Generate charts using Plotly Express (standard approach)."""

    SUPPORTED_CHARTS = ['bar', 'line', 'scatter', 'pie', 'histogram', 'box', 'area']

    @staticmethod
    def create_chart(df: pd.DataFrame, chart_type: str, x_column: str | None = None,
                    y_column: str | None = None, title: str = "Chart",
                    **kwargs) -> str:
        """
        Create a Plotly chart and return HTML.

        Returns: HTML string with interactive chart.
        """
        try:
            import plotly.express as px
        except ImportError:
            raise ImportError("Plotly required: pip install plotly")

        # Sample large datasets
        if len(df) > 10000:
            df = df.sample(n=10000, random_state=42)

        fig = PlotlyChartGenerator._create_fig(
            df, chart_type, x_column, y_column, title, **kwargs
        )

        # Update layout
        fig.update_layout(
            template=kwargs.get('template', 'plotly'),
            width=kwargs.get('width', 1200),
            height=kwargs.get('height', 600),
            title_font_size=kwargs.get('title_size', 20),
        )

        return fig.to_html(include_plotlyjs='cdn', full_html=True)

    @staticmethod
    def _create_fig(df: pd.DataFrame, chart_type: str, x_column: str | None,
                   y_column: str | None, title: str, **kwargs):
        """Internal method to create Plotly figure."""
        import plotly.express as px

        # Determine columns
        x_col = x_column or df.columns[0]
        y_col = y_column

        if chart_type == 'bar':
            if not y_col:
                y_col = df.select_dtypes(include=['number']).columns[0] if len(df.select_dtypes(include=['number']).columns) > 0 else df.columns[1]

            # Aggregate if many rows
            if len(df) > 100:
                df = df.groupby(x_col, as_index=False)[y_col].sum().sort_values(y_col, ascending=False)

            return px.bar(df, x=x_col, y=y_col, title=title,
                         color_discrete_sequence=kwargs.get('colors', None))

        elif chart_type == 'line':
            if not y_col:
                y_col = df.select_dtypes(include=['number']).columns[0] if len(df.select_dtypes(include=['number']).columns) > 0 else df.columns[1]

            # Sort for time series
            if pd.api.types.is_datetime64_any_dtype(df[x_col]):
                df = df.sort_values(x_col)

            return px.line(df, x=x_col, y=y_col, title=title, markers=kwargs.get('markers', True))

        elif chart_type == 'scatter':
            if not y_col:
                numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                x_col = numeric_cols[0] if len(numeric_cols) > 0 else df.columns[0]
                y_col = numeric_cols[1] if len(numeric_cols) > 1 else None

            return px.scatter(df, x=x_col, y=y_col, title=title,
                            color=kwargs.get('color_column'),
                            size=kwargs.get('size_column'),
                            hover_data=kwargs.get('hover_data'))

        elif chart_type == 'pie':
            if not y_col:
                y_col = df.select_dtypes(include=['number']).columns[0] if len(df.select_dtypes(include=['number']).columns) > 0 else df.columns[1]

            # Aggregate if needed
            if len(df) > 20:
                df = df.groupby(x_col, as_index=False)[y_col].sum()

            return px.pie(df, names=x_col, values=y_col, title=title)

        elif chart_type == 'histogram':
            return px.histogram(df, x=x_col, title=title, nbins=kwargs.get('bins', 30))

        elif chart_type == 'box':
            return px.box(df, x=x_col if x_col else None, y=y_col, title=title)

        elif chart_type == 'area':
            if not y_col:
                y_col = df.select_dtypes(include=['number']).columns[0] if len(df.select_dtypes(include=['number']).columns) > 0 else df.columns[1]

            return px.area(df, x=x_col, y=y_col, title=title)

        else:
            raise ValueError(f"Unsupported chart type: {chart_type}. Use: {', '.join(PlotlyChartGenerator.SUPPORTED_CHARTS)}")


# =============================================================================
# Main CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Generate Plotly charts from data (standard approach)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-recommend chart type
  python chart_visualizer.py --data sales.csv --auto --output analysis.html

  # Bar chart
  python chart_visualizer.py --data sales.csv --chart-type bar --x-column product --y-column revenue

  # Line chart for time series
  python chart_visualizer.py --data monthly.csv --chart-type line --x-column date --y-column sales

  # Scatter plot
  python chart_visualizer.py --data data.csv --chart-type scatter --x-column price --y-column demand
        """
    )

    # Input
    parser.add_argument('--data', required=True, help='Path to data file (CSV, JSON, Excel)')
    parser.add_argument('--format', default='csv', choices=['csv', 'json', 'excel'],
                       help='Input format (default: csv)')

    # Chart options
    parser.add_argument('--chart-type', choices=PlotlyChartGenerator.SUPPORTED_CHARTS,
                       help='Chart type (use --auto to recommend)')
    parser.add_argument('--auto', action='store_true', help='Auto-recommend chart type')

    # Columns
    parser.add_argument('--x-column', help='X axis column')
    parser.add_argument('--y-column', help='Y axis column')
    parser.add_argument('--color-column', help='Color encoding column')
    parser.add_argument('--size-column', help='Size encoding column (scatter)')

    # Style
    parser.add_argument('--title', default='Chart', help='Chart title')
    parser.add_argument('--width', type=int, default=1200, help='Width (default: 1200)')
    parser.add_argument('--height', type=int, default=600, help='Height (default: 600)')
    parser.add_argument('--template', default='plotly', help='Plotly template')

    # Output
    parser.add_argument('--output', '-o', default='chart.html',
                        help='Output file path (default: chart.html)')

    args = parser.parse_args()

    # Load data
    print(f"Loading: {args.data}")
    if args.format == 'csv':
        df = pd.read_csv(args.data)
    elif args.format == 'json':
        df = pd.read_json(args.data)
    elif args.format == 'excel':
        df = pd.read_excel(args.data)
    else:
        df = pd.read_csv(args.data)

    print(f"  Rows: {len(df)}, Columns: {len(df)}")

    # Determine chart type
    if args.auto:
        chart_type = ChartTypeSelector.recommend_chart(df, args.x_column, args.y_column)
        print(f"  Recommended: {chart_type}")
    else:
        chart_type = args.chart_type or ChartTypeSelector.recommend_chart(df, args.x_column, args.y_column)
        print(f"  Chart type: {chart_type}")

    # Suggest columns if not specified
    if not args.x_column or not args.y_column:
        x_sug, y_sug = ChartTypeSelector.suggest_columns(df, chart_type)
        if not args.x_column and x_sug:
            args.x_column = x_sug
        if not args.y_column and y_sug:
            args.y_column = y_sug

    if args.x_column:
        print(f"  X: {args.x_column}")
    if args.y_column:
        print(f"  Y: {args.y_column}")

    # Generate chart
    print("Generating...")
    html = PlotlyChartGenerator.create_chart(
        df, chart_type, args.x_column, args.y_column,
        title=args.title,
        width=args.width,
        height=args.height,
        template=args.template,
        color_column=args.color_column,
        size_column=args.size_column,
    )

    # Save
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"✓ Saved to: {output_path}")


if __name__ == '__main__':
    main()
