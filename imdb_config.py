import os

PATH_SCRAPY_TOOL = '/home/guijideanhao/pyproject/scrapy_toolv2'
DATA_DIRECTORY = '/home/guijideanhao/pyproject/imdb/imdb_data'
FILEPATH_DATABASE = os.path.join(DATA_DIRECTORY,'imdb_film.db')
FILEPATH_DATABASE2 = os.path.join(DATA_DIRECTORY,'imdb_film_for_title.db')
FILEPATH_DATABASE3 = os.path.join(DATA_DIRECTORY,'imdb_film_for_credit.db')
PATH_FILMLIST_TEMP = os.path.join(DATA_DIRECTORY,'filmlist_temp')
if not os.path.exists(PATH_FILMLIST_TEMP):
    os.makedirs(PATH_FILMLIST_TEMP)

TABLENAME_FILMLIST = 'film_list'    
TABLENAME_TITLE_BASIC = 'film_basic'
TABLENAME_FILM_CREW = 'film_crew'
TABLENAME_FILE_CONNECTIONS = 'film_connections'
TABLENAME_FILE_COMPANYCREDIT = 'company_credit'
FILEPATH_USEDURL_LI_TT = os.path.join(DATA_DIRECTORY,'used_url_li_tt.txt')
if not os.path.exists(FILEPATH_USEDURL_LI_TT):
    with open(FILEPATH_USEDURL_LI_TT,'a+') as f:
        f.write(' ,')
FILEPATH_USEDURL_TITLE = os.path.join(DATA_DIRECTORY, 'used_url_title.txt')
if not os.path.exists(FILEPATH_USEDURL_TITLE):
    with open(FILEPATH_USEDURL_TITLE,'a+') as f:
        f.write(' ,')
FILEPATH_ERROR_TITLE = os.path.join(DATA_DIRECTORY, 'error_url_title.txt')
if not os.path.exists(FILEPATH_ERROR_TITLE):
    with open(FILEPATH_ERROR_TITLE,'a+') as f:
        f.write(' ,')
FILEPATH_USEDTTID_CONNECTION = os.path.join(DATA_DIRECTORY, 'used_ttid_connection.txt')
if not os.path.exists(FILEPATH_USEDTTID_CONNECTION):
    with open(FILEPATH_USEDTTID_CONNECTION, 'a+') as f:
        f.write(' ,')
FILEPATH_ERRORTTID_CONNECTION = os.path.join(DATA_DIRECTORY, 'error_ttid_connection.txt')
if not os.path.exists(FILEPATH_ERRORTTID_CONNECTION):
    with open(FILEPATH_ERRORTTID_CONNECTION, 'a+') as f:
        f.write(' ,')

FILEPATH_LOGFILE_COMPANY_CREDIT = os.path.join(DATA_DIRECTORY, 'log_company_credit.txt')        
        
domain_url = 'https://www.imdb.com/'
genre_url_list = [
    'https://www.imdb.com/search/title?genres=action&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_1',
    'https://www.imdb.com/search/title?genres=documentary&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_7',
    'https://www.imdb.com/search/title?genres=horror&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_13',
    'https://www.imdb.com/search/title?genres=short&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_19',
    'https://www.imdb.com/search/title?genres=adventure&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_2',
    'https://www.imdb.com/search/title?genres=drama&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_8',
    'https://www.imdb.com/search/title?genres=music&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_14',
    'https://www.imdb.com/search/title?genres=sport&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_20',
    'https://www.imdb.com/search/title?genres=animation&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_3',
    'https://www.imdb.com/search/title?genres=family&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_9',
    'https://www.imdb.com/search/title?genres=musical&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_15',
    'https://www.imdb.com/search/keyword?keywords=superhero&title_type=movie&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_21',
    'https://www.imdb.com/search/title?genres=biography&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_4',
    'https://www.imdb.com/search/title?genres=fantasy&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_10',
    'https://www.imdb.com/search/title?genres=mystery&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_16',
    'https://www.imdb.com/search/title?genres=thriller&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_22',
    'https://www.imdb.com/search/title?genres=comedy&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_5',
    'https://www.imdb.com/search/title?genres=film-noir&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_11',
    'https://www.imdb.com/search/title?genres=romance&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_17',
    'https://www.imdb.com/search/title?genres=war&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_23',
    'https://www.imdb.com/search/title?genres=crime&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_6',
    'https://www.imdb.com/search/title?genres=history&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_12',
    'https://www.imdb.com/search/title?genres=sci-fi&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_18',
    'https://www.imdb.com/search/title?genres=western&title_type=feature&explore=genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=facfbd0c-6f3d-4c05-9348-22eebd58852e&pf_rd_r=0FW8EMRXJ56D9BKQDHGX&pf_rd_s=center-6&pf_rd_t=15051&pf_rd_i=genre&ref_=ft_gnr_mvpop_24'
]