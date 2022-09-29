import psycopg2 as pg
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# from datetime import datetime

def query_postgreSQL(query):
    conn = pg.connect(host='iconoci-postgresql.c0jviigby9cu.ap-northeast-2.rds.amazonaws.com', dbname = 'postgres', user = 'onomaaai_iconoci', password = 'cKnBhtQimTkoifv', port = 5432)
    # Get a DataFrame
    query_result = pd.read_sql(query, conn)
    # Close connection
    # start_tm = datetime.now()
    # end_tm = datetime.now()
    # print('START: ', str(start_tm))
    # print('END: ', str(end_tm))
    # print('ELAP: ', str(end_tm - start_tm))
    conn.close()

    return query_result


@st.experimental_memo(ttl=1)
def read_db(table, colum='*', schema='public'):
    query = f'SELECT {colum} FROM {schema}."{table}"'
    return query


def func_content(pct, allvals):
    absolute = int(np.round(pct/100.*np.sum(allvals)))
    return '{:d}\n{:.1f}%'.format(absolute, pct)

st.title('Statistics of ICONOCI')
st.header('Ratio of Registered to Unregistered')
with st.expander('See Charts'):

    col1, col2 = st.columns(2)

    #Ratio of Registered to Unregistered users in artify
    reg_artify_query = read_db(table='ArtifyRequest')
    reg_artify_df = query_postgreSQL(reg_artify_query)
    ratio_unreg_artify = reg_artify_df['userId'].count(None)/len(reg_artify_df)
    num_unreg_artify = reg_artify_df['userId'].count(None)
    total_num_reg_artify_df = len(reg_artify_df)

    labels_ra = 'Registered', 'Unregistered'
    sizes_ra = [len(reg_artify_df)-num_unreg_artify, num_unreg_artify]
    explode = (0.01, 0)

    fig_ra, ax_ra = plt.subplots()
    ax_ra.pie(sizes_ra, explode=explode,labels=labels_ra, startangle=90, autopct=lambda pct: func_content(pct, sizes_ra))
    ax_ra.axis('equal')
    plt.title(f'Ratio of Registered to Unregistered users in artify \n (total: {total_num_reg_artify_df})')
    col1.pyplot(fig_ra)

    fig_ra.savefig(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_artify_df}).png')
    with open(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_artify_df}).png', "rb") as file:
        btn = col1.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_artify_df}).png',
                mime="image/png"
            )
    # print('-'*50, 'ARTIFY','total_num :', len(reg_artify_df), '-'*50)
    # print('Registered :', len(reg_artify_df) - num_unreg_artify, '(' + str(100 - round(ratio_unreg_artify,3)*100), '%)')
    # print('Unregistered :', num_unreg_artify, '(' + str(round(ratio_unreg_artify,3)*100), '%)')


    #Ratio of Registered to Unregistered users in generation
    reg_generation_query = read_db(table = 'IconGenerationRequest')
    reg_generation_df = query_postgreSQL(reg_generation_query)
    ratio_unreg_generation = reg_generation_df['userId'].count(None)/len(reg_generation_df)
    num_unreg_generation = reg_generation_df['userId'].count(None)
    total_num_reg_generation_df = len(reg_generation_df)

    labels_rg = 'Registered', 'Unregistered'
    sizes_rg = [len(reg_generation_df)-num_unreg_generation, num_unreg_generation]

    fig_rg, ax_rg = plt.subplots()
    ax_rg.pie(sizes_rg, labels=labels_rg, explode=explode, startangle=90, autopct=lambda pct: func_content(pct, sizes_rg))
    ax_rg.axis('equal')
    plt.title(f'Ratio of Registered to Unregistered users in generation \n (total: {total_num_reg_generation_df})')
    col2.pyplot(fig_rg)

    fig_rg.savefig(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png')
    with open(f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png', "rb") as file:
        btn = col2.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Registered to Unregistered users in generation (total: {total_num_reg_generation_df}).png',
                mime="image/png"
            )
    # print('-'*50,'GENERATION','total num :', len(reg_generation_df),'-'*50)
    # print('Registered :', len(reg_generation_df) - num_unreg_generation , '(' + str(100 - round(ratio_unreg_generation,3)*100), '%)')
    # print('Unregistered :', num_unreg_generation, '(' + str(round(ratio_unreg_generation,3)*100), '%)')


#Ratio of Generation to Artify
st.header('Ratio of Generation to Artify')
with st.expander('See Charts'):
    col3, col4 = st.columns(2)

    labels_nga = 'Generation' , 'Artify'
    sizes_nga = [total_num_reg_generation_df, total_num_reg_artify_df]
    total_num_gen_art_df = total_num_reg_generation_df + total_num_reg_artify_df 
    fig_nga, ax_nga = plt.subplots()
    ax_nga.pie(sizes_nga, explode=explode, labels=labels_nga, startangle=90, autopct=lambda pct: func_content(pct, sizes_nga))
    plt.title(f'Ratio of Generation to Artify \n (total: {total_num_gen_art_df})')
    col3.pyplot(fig_nga)

    fig_nga.savefig(f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png')
    with open(f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png', "rb") as file:
        btn = col3.download_button(
                label="Download image",
                data=file,
                file_name=f'Ratio of Generation to Artify (total: {total_num_gen_art_df}).png',
                mime="image/png"
            )
