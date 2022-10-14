import psycopg2 as pg
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
from datetime import date
from datetime import timedelta



def query_postgreSQL(query):
    conn = pg.connect(host='iconoci-postgresql.c0jviigby9cu.ap-northeast-2.rds.amazonaws.com', dbname = 'postgres', user = 'onomaaai_iconoci', password = 'cKnBhtQimTkoifv', port = 5432)
    # Get a DataFrame
    query_result = pd.read_sql(query, conn)
    # Close connection
    conn.close()
    return query_result

def read_db(table, colum='*', schema='public'):
    query = f'SELECT {colum} FROM {schema}."{table}"'
    return query

def read_date_db(table, date, colum='*', schema='public'):
    query = f'''select count({colum}) from {schema}."{table}" where "timestamp" < '{date}';'''
    return query

def func_content(pct, allvals):
    absolute = int(np.round(pct/100.*np.sum(allvals)))
    return '{:d}\n{:.1f}%'.format(absolute, pct)


explode = (0.01, 0)

dd = dt.datetime.now() + timedelta(hours=9)

st.title('Statistics of ICONOCI')

if st.button('Click to Refresh'):
    st.experimental_rerun()

user_query = read_db(table = 'User')
user_df = query_postgreSQL(user_query)
generation_query = read_db(table = 'IconGenerationRequest')
generation_df = query_postgreSQL(generation_query)
artify_query = read_db(table='ArtifyRequest')
artify_df = query_postgreSQL(artify_query)
search_query = read_db(table='IconSearchRequest')
search_df = query_postgreSQL(search_query)
total_num_queries = len(generation_df) + len(artify_df) + len(search_df)


col0_1, col0_2 = st.columns(2)
col0_1.header(f'{dd.year}-{dd.month}-{dd.day} {dd.hour}:{dd.minute}')

#accumulated counts of generation query
today = date.today()
diff = (today.weekday() - 5) % 7
last_sat = today - timedelta(days=diff)

acc_gen_count_0 = query_postgreSQL(read_date_db('IconGenerationRequest',today + timedelta(days=1)))
acc_gen_count_1 = query_postgreSQL(read_date_db('IconGenerationRequest',last_sat))
acc_gen_count_2 = query_postgreSQL(read_date_db('IconGenerationRequest',last_sat - timedelta(days=7)))
acc_gen_count_3 = query_postgreSQL(read_date_db('IconGenerationRequest',last_sat - timedelta(days=7*2)))
acc_gen_count_4 = query_postgreSQL(read_date_db('IconGenerationRequest',last_sat - timedelta(days=7*3)))

acc_art_count_0 = query_postgreSQL(read_date_db('ArtifyRequest',today + timedelta(days=1)))
acc_art_count_1 = query_postgreSQL(read_date_db('ArtifyRequest',last_sat))
acc_art_count_2 = query_postgreSQL(read_date_db('ArtifyRequest',last_sat - timedelta(days=7)))
acc_art_count_3 = query_postgreSQL(read_date_db('ArtifyRequest',last_sat - timedelta(days=7*2)))
acc_art_count_4 = query_postgreSQL(read_date_db('ArtifyRequest',last_sat - timedelta(days=7*3)))

acc_src_count_0 = query_postgreSQL(read_date_db('IconSearchRequest',today + timedelta(days=1)))
acc_src_count_1 = query_postgreSQL(read_date_db('IconSearchRequest',last_sat))
acc_src_count_2 = query_postgreSQL(read_date_db('IconSearchRequest',last_sat - timedelta(days=7)))
acc_src_count_3 = query_postgreSQL(read_date_db('IconSearchRequest',last_sat - timedelta(days=7*2)))
acc_src_count_4 = query_postgreSQL(read_date_db('IconSearchRequest',last_sat - timedelta(days=7*3)))

acc_count_0 = acc_gen_count_0['count'][0] + acc_art_count_0['count'][0] + acc_src_count_0['count'][0]
acc_count_1 = acc_gen_count_1['count'][0] + acc_art_count_1['count'][0] + acc_src_count_1['count'][0]
acc_count_2 = acc_gen_count_2['count'][0] + acc_art_count_2['count'][0] + acc_src_count_2['count'][0]
acc_count_3 = acc_gen_count_3['count'][0] + acc_art_count_3['count'][0] + acc_src_count_3['count'][0]
acc_count_4 = acc_gen_count_4['count'][0] + acc_art_count_4['count'][0] + acc_src_count_4['count'][0]

labels_acc = [str(last_sat - timedelta(days=22)), str(last_sat - timedelta(days=15)), str(last_sat - timedelta(days=8)), str(last_sat - timedelta(days=1)), str(today)+'\nTODAY']
sizes_acc = [acc_count_4, acc_count_3, acc_count_2, acc_count_1, acc_count_0]

fig_acc, ax_acc = plt.subplots()
plt.title('Accumulated Counts of Total Activities')
plt.plot(labels_acc, sizes_acc, marker='o')
ax_acc.spines['right'].set_visible(False)
ax_acc.spines['top'].set_visible(False)
ax_acc.spines['left'].set_visible(False)
ax_acc.get_yaxis().set_visible(False)
for i in range(len(labels_acc)):
    height = sizes_acc[i]
    plt.text(labels_acc[i], height + 5, '%.0f' %height, ha='center', va='bottom', size = 12)
col0_1.pyplot(fig_acc)

col0_2.header(f'Total Users : {len(user_df)} (14)')
col0_2.text(f'Total Activities : {total_num_queries} \nTotal Generation : {len(generation_df)} \nTotal Artify : {len(artify_df)} \nTotal Search : {len(search_df)}')

fig_acc.savefig(f'Accumulated Counts of Total Activities {dd.year}-{dd.month}-{dd.day}.png')
with open(f'Accumulated Counts of Total Activities {dd.year}-{dd.month}-{dd.day}.png', "rb") as file:
    btn = col0_2.download_button(
            label="Download image",
            data=file,
            file_name=f'Accumulated Counts of Total Activities {dd.year}-{dd.month}-{dd.day}.png',
            mime="image/png"
        )

# top 10 queries 

st.header('Top 10 queries')
with st.expander('See Charts'):
    query_dic = {}
    for q in generation_df['query']:
        q = q.lower()
        if q not in query_dic:
            query_dic[q] = 1
        else: query_dic[q] += 1
    d = dict(sorted(query_dic.items(),key=lambda x:x[1], reverse=True))
    t, s = d.keys(), d.values()
    t = list(t)
    s = list(s)
    t_10 = t[0:10]
    s_10 = s[0:10]
    fig_q, ax_q = plt.subplots()
    ax_q.get_yaxis().set_visible(False)
    plt.xticks(rotation=90)
    bar = ax_q.bar(t_10, s_10, color='#6ea180')
    for rect in bar:
        height = rect.get_height()
        ax_q.text(rect.get_x() + rect.get_width()/2.0, height, '%.1f' % height, ha='center', va='bottom', size = 8)
    st.pyplot(fig_q)

    fig_q.savefig(f'Top 10 queries {dd.year}-{dd.month}-{dd.day} {dd.hour}-{dd.minute}.png', bbox_inches = 'tight')
    with open(f'Top 10 queries {dd.year}-{dd.month}-{dd.day} {dd.hour}-{dd.minute}.png', "rb") as file:
        btn = st.download_button(
                label="Download image",
                data=file,
                file_name=f'Top 10 queries {dd.year}-{dd.month}-{dd.day} {dd.hour}-{dd.minute}.png',
                mime="image/png"
            )

# Ratio of Generation to Artify
total_num_reg_artify_df = len(artify_df)
total_num_reg_generation_df = len(generation_df)

st.header('Ratio of Generation to Artify')
with st.expander('See Charts'):
    col2_1, col2_2 = st.columns(2)

    labels_nga = 'Artify', 'Generation'
    sizes_nga = [total_num_reg_artify_df, total_num_reg_generation_df]
    total_num_gen_art_df = total_num_reg_generation_df + total_num_reg_artify_df 

    fig_nga, ax_nga = plt.subplots()
    ax_nga.pie(sizes_nga, explode=explode, labels=labels_nga, startangle=90, autopct=lambda pct: func_content(pct, sizes_nga), colors=['#f9c0c7','#a8c8f9'])
    plt.title(f'Ratio of Generation to Artify \n (total: {total_num_gen_art_df})')
    col2_1.pyplot(fig_nga)

    fig_nga.savefig(f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png')
    with open(f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png', "rb") as file:
        btn = col2_1.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png',
                mime="image/png"
            )

# Ratio of Registered to Unregistered
st.header('Ratio of Registered to Unregistered')
with st.expander('See Charts'):

    col1_1, col1_2 = st.columns(2)

# Ratio of Registered to Unregistered users in artify

    ratio_unreg_artify = artify_df['userId'].count(None)/len(artify_df)
    num_unreg_artify = artify_df['userId'].count(None)

    labels_ra = 'Registered', 'Unregistered'
    sizes_ra = [num_unreg_artify, len(artify_df)-num_unreg_artify]

    fig_ra, ax_ra = plt.subplots()
    ax_ra.pie(sizes_ra, explode=explode,labels=labels_ra, startangle=90, autopct=lambda pct: func_content(pct, sizes_ra), colors=['#c1abff','#80afe5'])
    ax_ra.axis('equal')
    plt.title(f'Ratio of Registered to Unregistered users in artify \n (total: {total_num_reg_artify_df})')
    col1_1.pyplot(fig_ra)

    fig_ra.savefig(f'Ratio of Registered to Unregistered users in artify (total: {total_num_reg_artify_df}).png')
    with open(f'Ratio of Registered to Unregistered users in artify (total: {total_num_reg_artify_df}).png', "rb") as file:
        btn = col1_1.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Registered to Unregistered users in artify (total: {total_num_reg_artify_df}).png',
                mime="image/png"
            )


# Ratio of Registered to Unregistered users in generation
    
    ratio_unreg_generation = generation_df['userId'].count(None)/len(generation_df)
    num_unreg_generation = generation_df['userId'].count(None)

    labels_rg = 'Registered', 'Unregistered'
    sizes_rg = [num_unreg_generation, len(generation_df)-num_unreg_generation]

    fig_rg, ax_rg = plt.subplots()
    ax_rg.pie(sizes_rg, labels=labels_rg, explode=explode, startangle=90, autopct=lambda pct: func_content(pct, sizes_rg), colors=['#c1abff','#80afe5'])
    ax_rg.axis('equal')
    plt.title(f'Ratio of Registered to Unregistered users in generation \n (total: {total_num_reg_generation_df})')
    col1_2.pyplot(fig_rg)


    fig_rg.savefig(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png')
    with open(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png', "rb") as file:
        btn = col1_2.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png',
                mime="image/png"
            )




# Ratio of "Reached out to Artify" to "Only Generation"

st.header('Ratio of "Reached out to Artify" to "Only Generation"')
with st.expander('See Charts'):
    col4_1, col4_2 = st.columns(2)
    total_num_generation = len(generation_df)
    num_org_gen = len(artify_df[artify_df['iconOrigin'] == 'generate'])
    rst_num_org_gen = total_num_generation - num_org_gen

    labels_rraog = 'Generation & Artify', 'Only Generation'
    sizes_rraog = [num_org_gen, rst_num_org_gen]

    fig_rraog, ax_rraog = plt.subplots()
    ax_rraog.pie(sizes_rraog, explode=explode, labels=labels_rraog, startangle=90, autopct=lambda pct: func_content(pct, sizes_rraog), colors=['#6ea150', '#868cbb'])
    plt.title(f'Ratio of "Reached out to Artify" to "Only Generation" \n (total: {total_num_generation})')
    col4_1.pyplot(fig_rraog)

    fig_rraog.savefig(f'Ratio of "Reached out to Artify" to "Only Generation" (total: {total_num_generation}).png')
    with open(f'Ratio of "Reached out to Artify" to "Only Generation" (total: {total_num_generation}).png', 'rb') as file:
        btn = st.download_button(label="Download image",
            data=file,
            file_name=f'Ratio of "Reached out to Artify" to "Only Generation" (total: {total_num_generation}).png',
            mime="image/png"
        )

# Ratio of Download
st.header('Ratio of Download')
with st.expander('See Charts'):
    col3_1, col3_2 = st.columns(2)

# Ratio of Download to Generation
    num_dl_gen = len(generation_df[generation_df['downloadCount']==1])

    labels_dl = 'Download', 'Non-Download'
    sizes_dl_gen = [num_dl_gen, len(generation_df)-num_dl_gen]
    
    fig_dl_gen, ax_dl_gen = plt.subplots()
    ax_dl_gen.pie(sizes_dl_gen, explode=explode, labels=labels_dl, startangle=90, autopct=lambda pct: func_content(pct, sizes_dl_gen), colors=['#ffa9b0','#bbb8dc'])
    plt.title(f'Ratio of Download from Generation \n (total: {len(generation_df)})')
    col3_1.pyplot(fig_dl_gen)

    fig_dl_gen.savefig(f'Ratio of Download from Generation (total: {len(generation_df)}).png')
    with open(f'Ratio of Download from Generation (total: {len(generation_df)}).png', 'rb') as file:
        btn = col3_1.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Download from Generation (total: {len(generation_df)}).png',
                mime="image/png"
            )

# Ratio of Download to Search
    num_dl_src = len(search_df[search_df['downloadCount']==1])
    sizes_dl_src = [num_dl_src, len(search_df)-num_dl_src]

    fig_dl_src, ax_dl_src = plt.subplots()
    ax_dl_src.pie(sizes_dl_src, explode=explode, labels=labels_dl, startangle=90, autopct=lambda pct: func_content(pct, sizes_dl_src), colors=['#ffa9b0','#bbb8dc'])
    plt.title(f'Ratio of Download from Search \n (total: {len(search_df)})')
    col3_2.pyplot(fig_dl_src)

    fig_dl_src.savefig(f'Ratio of Download from Search (total: {len(search_df)}).png')
    with open(f'Ratio of Download from Search (total: {len(search_df)}).png', 'rb') as file:
        btn = col3_2.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Download from Search (total: {len(search_df)}).png',
                mime="image/png"
            )
