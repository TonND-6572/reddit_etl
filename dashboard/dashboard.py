import streamlit as st
import pandas as pd
import plotly.express as px
import altair as alt
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.aws_glue_service import get_from_data_catalog, connect_to_botos3, extract_data

st.set_page_config(
    page_title="Reddit Data Engineer Session Dashboard",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded"
)
alt.themes.enable("dark")

# Data Connection
client = connect_to_botos3('athena')
res = get_from_data_catalog(client)
try:
    df = extract_data(res)
except Exception as e:
    df = pd.DataFrame()  # Fallback in case of failure

# Data Transformation and Validation
def transform_n_validate(df):
    df['created_utc'] = pd.to_datetime(df['created_utc'])
    df['year'] = df['created_utc'].dt.year
    df['month'] = df['created_utc'].dt.month
    df['day'] = df['created_utc'].dt.day
    df['score'] = df['score'].astype('int')
    df = df[~(df['url'] == "https://www.bytebase.com/blog/bytebase-3-0/")]
    df.drop_duplicates(['id'], keep='first', inplace=True)
    return df

if not df.empty:
    df = transform_n_validate(df)

# Sidebar
with st.sidebar:
    st.title('🏂 Reddit Data Engineer Session Dashboard')
    month_list = sorted(df['month'].unique(), reverse=True) if 'month' in df else []
    month_list.append('All months')
    selected_month = st.selectbox('Select a month', month_list, index=len(month_list)-1)
    df_selected_month = df[df['month'] == selected_month] if selected_month != 'All months' else df

    # Sort by Date
    df_selected_month = df_selected_month.sort_values(['year', 'month', 'day'], ascending=[False, False, False])

# Main Dashboard
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown('#### Score vs. Number of Comments')
    df_filtered = df_selected_month[df_selected_month['score'] < 500]
    scatter_fig = px.scatter(df_filtered, x='score', y='num_comments', trendline='ols', height=350)
    st.plotly_chart(scatter_fig, use_container_width=True)

    if selected_month == 'All months':
        st.markdown('#### Post over month')
        group_by_month = df.groupby(['year','month']).agg(
            number_of_posts=('title', 'count')
        ).reset_index()
        line_fig = px.line(group_by_month, x='month', y='number_of_posts', height=250)
        st.plotly_chart(line_fig, use_container_width=True)
    else:
        st.markdown('#### Post over day')
        cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df_daily = df_selected_month[df_selected_month['created_utc'] < cutoff]
        group_by_date = df_daily.groupby(['year', 'month', 'day']).agg(
            number_of_posts=('title', 'count')
        ).reset_index()
        line_fig = px.line(group_by_date, x='day', y='number_of_posts', height=250)
        line_fig.update_xaxes(dtick=1)
        st.plotly_chart(line_fig, use_container_width=True)

with col2:
    # Metrics and Top 5 Listings
    st.metric(label='Number of Users', value=df['author'].nunique() if not df.empty else 0)

    # Top 5 Users
    st.markdown("### Top 5 Users by Posts and Score")
    group_by_user = df_selected_month.groupby('author').agg(
        number_of_posts=('title', pd.Series.nunique),
        average_score=('score', 'mean')
    ).sort_values(['number_of_posts', 'average_score'], ascending=[False, False]).head(5)
    st.dataframe(group_by_user)

    # Recent Posts
    st.markdown("### 5 Most Recent Posts")
    st.dataframe(df_selected_month[['created_utc', 'title', 'author', 'url']].nlargest(5, 'created_utc'))
    
    # Top 5 Posts by Score
    st.markdown("### Top 5 Posts by Score")
    st.dataframe(df_selected_month[['title', 'score', 'url']].nlargest(5, 'score'))

    
# Explore Data
st.markdown('## Explore Data')
st.dataframe(df_selected_month.sort_values('created_utc', ascending=False).drop(['year', 'month', 'day'], axis=1))