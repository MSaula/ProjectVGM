import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sn
import numpy as np

from statsmodels.tsa.seasonal import seasonal_decompose, STL
from sklearn.linear_model import LinearRegression
from scipy.fft import fft, fftfreq


class CustomPlotter:

    @staticmethod
    def hist_and_boxplot(df, col, ax=None, outlier_drop_pct=0):
        if ax is None:
            fig, (ax_hist, ax_boxplot) = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), gridspec_kw={"height_ratios": (.85, .15), "hspace": 0.3})
        else:
            ax_hist, ax_boxplot = ax
        
        # Remove the top n% of data
        n_pct = 1 - (outlier_drop_pct / 100)
        df_ = df[df[col] <= df[col].quantile(n_pct)]
        if len(df_) < len(df) * n_pct: df_ = df

        # Histogram with a vertical line at the mean value
        sn.histplot(data=df_, x=col, kde=False, ax=ax_hist)
        ax_hist.axvline(df_[col].mean(), color='r', linestyle='--', label=f'Mean: {df_[col].mean():.2f}')
        ax_hist.legend()
        ax_hist.set_title(f'Histogram and Boxplot for {col}')
        ax_hist.set_xlabel('')

        # Horizontal boxplot below the histogram
        sn.boxplot(data=df_, x=col, orient='h', ax=ax_boxplot)
        ax_boxplot.set_xlabel('')
        ax_boxplot.set_yticks([])

    @staticmethod
    def top_barplot_cat(df, col, ax=None, top_n_categories=10):
        if ax is None:
            ax = plt.gca()

        category_counts = df[col].value_counts(ascending=False).iloc[:top_n_categories]  # Select the N most frequent categories
        sn.barplot(x=list(category_counts.index), y=category_counts.values, palette='viridis', ax=ax)
        ax.set_title(f'Barplot for {col} (Top {top_n_categories} Ordered by Frequency)')
        ax.set_xlabel(col)
        ax.set_ylabel('Frequency')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)  # Rotate labels to prevent overlap

    @staticmethod
    def num_vs_target(df: pd.DataFrame, col: str, target: str, ax: plt.Axes = None):
        # Create the scatter plot and regression line in the provided Axes object
        if ax is None:
            ax = plt.gca()

        # Create scatter plot with regression line
        sn.scatterplot(data=df, x=col, y=target, alpha=0.7, ax=ax)
        ax.set_title(f"Scatter plot of {col} vs {target}")

        # Calculate and plot the linear regression line
        no_nan_data = df.dropna()
        X = no_nan_data[col].values.reshape(-1, 1)
        Y = no_nan_data[target].values.reshape(-1, 1)
        model = LinearRegression()
        model.fit(X, Y)
        y_pred = model.predict(X)

        y_min = df[target].min()
        y_max = df[target].max()
        y_range = y_max - y_min

        y_min -= y_range * .02
        y_max += y_range * .02

        ax.set_ylim((y_min, y_max))

        ax.plot(X, y_pred, color="red", linestyle="--", label="Linear regression")
        ax.legend()

    @staticmethod
    def cat_vs_target(df: pd.DataFrame, col: str, target: str, ax: plt.Axes = None, log_attrs=[]):
        # Sort categories by mean value
        category_order = df.groupby(col)[target].median().sort_values(ascending=False).index

        # Create boxplot for categorical columns in the provided Axes object
        if ax is None:
            ax = plt.gca()

        boxplot = sn.boxplot(data=df, x=col, y=target, order=category_order, ax=ax)
        ax.set_title(f"Boxplot of {target} by {col}")

        # Tilt category labels and adjust font size
        num_categories = len(category_order)
        
        if col in log_attrs: ax.set_yscale('log')

        fontsize = 8 if num_categories > 10 else 10
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, fontsize=fontsize, ha="right")

    @staticmethod
    def combined_plots(df, columns, target, figsize=(14, 8), outlier_drop_pct=0, log_attrs=[]):
        sn.set_style('whitegrid')
        for col in columns:
            if col == target:
                continue
            fig = plt.figure(figsize=figsize)
            fig.suptitle(f'Analysis of {col}', fontsize=16)
            gs = fig.add_gridspec(6, 2)

            ax_target = fig.add_subplot(gs[:, 1])

            if pd.api.types.is_numeric_dtype(df[col]):
                ax_hist = fig.add_subplot(gs[:5, 0])
                ax_box = fig.add_subplot(gs[5, 0])
                CustomPlotter.hist_and_boxplot(df, col, ax=(ax_hist, ax_box), outlier_drop_pct=outlier_drop_pct)
                CustomPlotter.num_vs_target(df, col, target, ax=ax_target)
            else:
                ax_bp = fig.add_subplot(gs[:, 0])
                CustomPlotter.top_barplot_cat(df, col, ax=ax_bp, top_n_categories=10)
                CustomPlotter.cat_vs_target(df, col, target, ax=ax_target, log_attrs=log_attrs)
            
            plt.tight_layout()
            plt.show()
    
    @staticmethod
    def plot_histogram_boxplot(data, title, ax_hist, ax_box):
        sn.histplot(data=data, kde=True, ax=ax_hist)
        ax_hist.axvline(data.mean(), color='red', linestyle='--')
        ax_hist.set_title(title + ' Histogram')
        
        sn.boxplot(x=data, orient='h', ax=ax_box)
        ax_box.set_title(title + ' Boxplot')
        ax_box.set_yticks([])

    @staticmethod
    def find_optimal_period(data, min_period, max_period):
        best_period = None
        min_rss = np.inf

        for period in range(min_period, max_period + 1, 5):
            try:
                decomposition = seasonal_decompose(data, model='additive', period=period)
                residuals = decomposition.resid.dropna()
                rss = np.sum(residuals ** 2)

                if rss < min_rss:
                    min_rss = rss
                    best_period = period
            except ValueError:
                continue

        return best_period

    @staticmethod
    def plot_timeseries_decomposition(data, title, ax):
        min_period = 24 * 6  # 1 day in 10-minute intervals
        max_period = 90 * 24 * 6  # 1 month in 10-minute intervals
        optimal_period = CustomPlotter.find_optimal_period(data, min_period, max_period)

        decomposition = seasonal_decompose(data, model='additive', period=optimal_period)
        trend = decomposition.trend.dropna()
        seasonal = decomposition.seasonal.dropna()
        resid = decomposition.resid.dropna()

        ax[0].plot(data, label='Original')
        ax[0].set_title(title + ' Time Series [%d]' % optimal_period)
        
        ax[1].plot(trend, label='Trend')
        ax[1].set_title(title + ' Trend [%d]' % optimal_period)
        
        ax[2].plot(seasonal, label='Seasonality')
        ax[2].set_title(title + ' Seasonality [%d]' % optimal_period)
        
        ax[3].plot(resid, label='Residuals [%d]' % optimal_period)
        ax[3].set_title(title + ' Residuals')

    @staticmethod
    def plot_fft(data, title, ax):
        N = len(data)
        T = 10 * 60  # 10 minutes in seconds
        yf = fft(data.to_numpy())
        xf = fftfreq(N, T)[:N // 2]

        # Plot the FFT with logarithmic scale and real frequency (in Hz)
        ax.plot(xf, 20 * np.log10(2.0 / N * np.abs(yf[:N // 2])))
        ax.set_title(title + ' Frequency Domain')
        ax.set_xlabel('Frequency (Hz)')
        ax.set_ylabel('Amplitude (dB)')
    
    @staticmethod
    def time_series_analysis(df, attribute):
        if attribute == 'Date Time':
            return

        data = df[attribute].dropna()

        sn.set(style='whitegrid')
        fig = plt.figure(figsize=(16, 12))
        fig.suptitle(f'Analysis of {attribute}', fontsize=16)
        
        # Create subplots with different heights
        gs = fig.add_gridspec(6, 2)
        ax_hist = fig.add_subplot(gs[:3, 0])
        ax_box = fig.add_subplot(gs[3, 0])
        ax_ts = [fig.add_subplot(gs[i, 1]) for i in range(4)]
        ax_fft = fig.add_subplot(gs[4:, :])

        # Call plotting functions
        CustomPlotter.plot_histogram_boxplot(data, attribute, ax_hist, ax_box)
        CustomPlotter.plot_timeseries_decomposition(data, attribute, ax_ts)
        CustomPlotter.plot_fft(data, attribute, ax_fft)
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()


if __name__ == "__main__":
    data = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6], 'b': [6, 5, 4, 3, 2, 1]})
    CustomPlotter.time_series_analysis(data, 'a')
