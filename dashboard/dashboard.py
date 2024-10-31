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
    page_title="Reddit Data engineer session Dashboard",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")


#data 
client = connect_to_botos3('athena')
res = get_from_data_catalog(client)
try:
    df = extract_data(res)
except Exception as e:
    pass

col = st.columns((5, 3), gap='medium')

def transform_n_validate(df):
    df['created_utc'] = pd.to_datetime(df['created_utc'])#, '%Y-%m-%d %H:%M:%S')
    df['year'] = df['created_utc'].dt.year
    df['month'] = df['created_utc'].dt.month
    df['day'] = df['created_utc'].dt.day

    df['score'] = df['score'].astype('int')
    df = df[~(df['url'] == "https://www.bytebase.com/blog/bytebase-3-0/")]
    df.drop_duplicates(['id'], keep='first', inplace=True)

    return df

df = transform_n_validate(df)


with st.sidebar:
    st.title('🏂 Reddit Data engineer session Dashboard')
    
    month_list = list(df['month'].unique())[::-1]
    month_list.append('All month')
    selected_month = st.selectbox('Select a month', month_list, index=len(month_list)-1)
    df_selected_month = df[df['month'] == selected_month] if not (selected_month == 'All month') else df
    df = df.sort_values(['year', 'month', 'day'], ascending=[False, False, False])


with col[0]:
    # score vs #comment
    st.header('Score vs # comment')
    ### remove outliner
    df_filter_interval = df[df['score'] < 500]
    scatter_fig = px.scatter(df_filter_interval, x = 'score', y = 'num_comments', trendline='ols')
    st.plotly_chart(scatter_fig)

    # number of post each day
    st.header('Number of post each day')
    cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    upto_last_date = df[df['created_utc'] < cutoff]
    group_by_date = upto_last_date.groupby(['year', 'month', 'day']) \
        .agg(number_of_author=('author', 'count'), number_of_post=('title', 'count'))
    group_by_date.reset_index(inplace=True)
    line_fig = px.line(group_by_date, 'day', 'number_of_post')
    st.plotly_chart(line_fig)

with col[1]:
    # number of user
    user_count = df['author'].nunique()
    st.metric(label='number of user', value=user_count)

    # top 5 post 
    st.markdown('### Top 5 post score')
    sort_by_score = df.sort_values(by=['score'], ascending=False)
    st.dataframe(sort_by_score[['title', 'score'    , 'url']].iloc[:5])

    # top 5 user 
    st.markdown('### Top 5 user')
    
    group_by_user = df.groupby('author') \
        .agg(
            number_of_post = ('title', pd.Series.nunique), 
            score_mean = ('score', 'mean')
        ) \
        .sort_values(['number_of_post', 'score_mean'], ascending=[False, False])

    st.dataframe(group_by_user[:5])

    # recent 5 post
    st.markdown('### Recent 5 post')
    sort_by_time = df.sort_values('created_utc', ascending=False)
    
    st.dataframe(sort_by_time[['created_utc', 'title', 'author', 'url']].iloc[:5])


st.markdown('## Explore data')
st.dataframe(df.sort_values('created_utc', ascending=False).drop(['year', 'month', 'day'], axis=1))