import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
def column_coverage(series: pd.Series, default_value: object) -> float:
    """
    Calculate the coverage of a column in percentage.
    :param series: Pandas Series
    :param default_value: Default value to be considered as missing
    :return: Coverage of the column in percentage
    """
    # Calculate the coverage of the column in percentage by dividing the number
    # of non-default values to the total number of values

    # If type of series is boolean then default value is False
    if series.dtype == 'boolean':
        # If the series is of boolean type, count the True values
        coverage = series.sum() / series.shape[0] * 100
    else:
        coverage = series[series != default_value].shape[0] / series.shape[0] * 100
    return coverage


def table_health(df: pd.DataFrame, default_value: str) -> pd.DataFrame:
    """
    Calculate the health metrics of a DataFrame.
    :param df: Pandas DataFrame
    :param default_value: Default value to be considered as missing
    :return: Health metrics of the DataFrame
    """
    # list to store the health metrics of the DataFrame
    list_table__health = []
    # Iterate over the columns of the DataFrame
    for column in df.columns:
        # Calculate the coverage of the column
        coverage = column_coverage(series=df[column], default_value=default_value)

        # Append the health metrics to the DataFrame
        list_table__health.append(
            dict(column_name=column,
                 coverage=coverage)
        )

    # Return the health metrics as a DataFrame sorted by coverage
    return pd.DataFrame(list_table__health).sort_values(by='coverage', ascending=False)


# Define the color mapping based on coverage
def get_color_label(coverage):
    """
    Get the color label based on the coverage percentage.
    :param coverage: Coverage percentage
    :return: Color label
    """
    if coverage > 90:
        return 'good'
    elif coverage > 70:
        return 'warning'
    else:
        return 'bad'


def plot_health_metrics(df_source: pd.DataFrame, table_name: str, default_value: str, palette: dict):
    """
    Plot the health metrics of a DataFrame.
    :param df_source: Pandas DataFrame to be analyzed
    :param table_name: Name of the table
    :param default_value: Default value to be considered as missing
    :param palette: Color palette
    """
    # Calculate the health metrics of the table
    df_health = table_health(df=df_source,
                             default_value=default_value)

    # Visualize the health metrics using a bar chart with the coverage on the y-axis.
    # We use 'good' color for coverage above 90%, 'warning' color for coverage between 70% and 90%
    # and 'bad' color for coverage below 70%.

    # Apply the function to create a new column for color labels
    df_health['color_label'] = df_health['coverage'].apply(get_color_label)

    # Create the bar plot with data labels with hue based on the color labels
    ax = sns.barplot(x='coverage', y='column_name', data=df_health,
                     hue='color_label', palette=palette, dodge=False, legend=False)
    # Add the data labels to the plot (truncated to 0 decimal points)
    for container in ax.containers:
        ax.bar_label(container, fmt='%.1f%%', label_type='edge')
    plt.xlabel('Coverage')
    plt.ylabel('Column')
    # Set the title of the plot
    plt.title(f'{table_name} - Health Metrics')
    plt.xlim(0, 100)
    plt.show()
