import requests,re,os,sqlite3,sys,time,json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from imdb_config import *
from urllib.parse import urljoin
sys.path.append('/home/guijideanhao/pyproject/scrapy_toolv2')
from html_downloader import html_downloader

class imdb_spider():
    def __init__(self,dbpath=FILEPATH_DATABASE2, hd=None):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(dbpath)
        self.cur = self.conn.cursor()
        if hd:
            self.hd = hd
        else:
            self.hd = html_downloader(china=False)
        self.filmlist_used_ttid()
#         self.used_url_li_tt = self.used_url_gen(FILEPATH_USEDURL_LI_TT)
        self.used_url_title = self.used_url_gen(FILEPATH_USEDURL_TITLE)
        self.error_url_title = self.used_url_gen(FILEPATH_ERROR_TITLE)
    
    def used_url_gen(self, filename):
        with open(filename, 'r') as f:
            tempstr = f.read()
        return list(tempstr.split(','))
    
    def used_url_add(self, url, used_list, filename):
        with open(filename, 'a+') as f:
            f.write('{},'.format(url))
        used_list.append(url)
        print('SUCCESS: scrape {}'.format(url))
        
    def filmlist_used_ttid(self):
        try:
            tempdf = pd.read_sql('select ttid from {}'.format(TABLENAME_FILMLIST), self.conn)
            self.used_filmlist = set(tempdf['ttid'].values.tolist())
        except:
            self.used_filmlist = set()
        
    def parse_li_tt(self, res_content):
        list_film_id = []
        soup = BeautifulSoup(res_content, 'html.parser')
        item_content = soup.find_all('div', {'class':'lister-item-content'})
        for i in item_content:
            item_header = i.find('h3', {'class':'lister-item-header'})
            film_url = item_header.a['href']
            film_name = item_header.a.get_text()
            s_film_id = re.search(r'tt\d+', film_url)
            film_id = s_film_id.group()
            try:
                item_genre = i.find('span', {'class':'genre'})
                film_genre = item_genre.get_text()
                film_genre = re.sub(r'\s','',film_genre)
                film_year = i.find('span', {'class':'lister-item-year'}).get_text()
                film_year = re.sub(r'\D', '', film_year)
            except:
                print("ERROR: {}".format(film_id))
        # save
            temp_dict = {'ttid':film_id, 'name':film_name, 'year':film_year, 'genre':film_genre, 'url':film_url}
            list_film_id.append(temp_dict)
        
        # next page
        item_next_page = soup.find('a', {'class':'lister-page-next'})
        if item_next_page:
            np_url = item_next_page['href']
        else:
            np_url = False
        return list_film_id,np_url
    def save_li_tt(self, list_film_id, to_db=True, table_name=TABLENAME_FILMLIST):
        dict_for_pandas = {'ttid':[], 'name':[], 'year':[], 'genre':[], 'url':[]}
        for i in list_film_id:
            if i['ttid'] in self.used_filmlist:
                continue
            for k in dict_for_pandas.keys():
                dict_for_pandas[k].append(i.get(k))
            self.used_filmlist.add(i['ttid'])
        df_list_film = pd.DataFrame(dict_for_pandas)
        
        if to_db:
            df_list_film.to_sql(name=table_name,con=self.conn,if_exists='append',index=False)
        return df_list_film
    def scrapy_li_tt(self, genre_url,teststop=-1):
        if teststop ==0:
            print('test end')
            return
        if teststop > 0:
            teststop = teststop-1
        if genre_url in self.used_url_li_tt:
            genre_url = self.used_url_li_tt[-2]
            print('ATTENTION: start from {}'.format(genre_url))
        response = self.hd.request_proxy(genre_url)
        if response:
            list_film_id,np_url = self.parse_li_tt(response.content)
            df_list_film = self.save_li_tt(list_film_id)
            df_list_film.to_csv(os.path.join(PATH_FILMLIST_TEMP,'{}.csv'.format(int(time.time()))), index=False)
            self.used_url_add(genre_url,self.used_url_li_tt,FILEPATH_USEDURL_LI_TT)
            if np_url:
                np_url = urljoin(domain_url, np_url)
                time.sleep(1)
                self.scrapy_li_tt(np_url, teststop=teststop)
        else:
            print('ERROR: scrapy interrupt!!!')
    
    def scrapy_li_tt_all(self):
        for i in genre_url_list:
            self.scrapy_li_tt(i)
    
    def parse_title(self, res_content):
        soup = BeautifulSoup(res_content, 'html.parser')
        script_data = soup.find('script', {'type':'application/ld+json'})
        if script_data:
            dict_film_basic, df_film_crew = self.parse_title_json(script_data.get_text())
        else:
            dict_film_basic = {}
            df_film_crew = pd.DataFrame()
        plot_summary = soup.find('div',{'class':'summary_text'})
        if plot_summary:
            dict_film_basic['plot_summary'] = plot_summary.get_text()
            dict_film_basic['plot_summary'] = re.sub('\s\s','',dict_film_basic['plot_summary'])
        storyline = soup.find('div', {'class': 'inline canwrap'})
        try:
            dict_film_basic['storyline'] = storyline.p.span.get_text()
            dict_film_basic['storyline'] = re.sub('\s\s','',dict_film_basic['storyline'])
        except:
            storyline = ''
        
        titleDetails = soup.find('div', {'class':'article', 'id':'titleDetails'})
        s_budget = re.search(r'Budget:</h4>["$ ]+[\d,]+',str(titleDetails))
        if s_budget:
            budget = s_budget.group()
            s_budget = re.search(r'["$ ]+[\d,]+',budget)
            dict_film_basic['budget'] = s_budget.group()
        
        s_open_weekend_usa = re.search(r'Opening Weekend USA:</h4>["$ ]+[\d,]+',str(titleDetails))
        if s_open_weekend_usa:
            open_weekend_usa = s_open_weekend_usa.group()
            s_open_weekend_usa = re.search(r'["$ ]+[\d,]+',open_weekend_usa)
            dict_film_basic['open_weekend_usa'] = s_open_weekend_usa.group()
        
        s_gross_usa = re.search(r'Gross USA:</h4>["$ ]+[\d,]+', str(titleDetails))
        if s_gross_usa:
            gross_usa = s_gross_usa.group()
            s_gross_usa = re.search(r'["$ ]+[\d,]+',gross_usa)
            dict_film_basic['gross_usa'] = s_gross_usa.group()
        
        s_cumulative_worldwide_gross = re.search(r'Cumulative Worldwide Gross:</h4>["$ ]+[\d,]+', str(titleDetails))
        if s_cumulative_worldwide_gross:
            cumulative_worldwide_gross = s_cumulative_worldwide_gross.group()
            s_cumulative_worldwide_gross = re.search(r'["$ ]+[\d,]+',cumulative_worldwide_gross)
            dict_film_basic['cumulative_worldwide_gross'] = s_cumulative_worldwide_gross.group()
        return dict_film_basic, df_film_crew
    
    def parse_title_json(self, json_str):
        dict_film_basic = {}
        tmp_dict = json.loads(json_str)
        dict_film_basic['title_url'] = tmp_dict.get('url')
        s_film_id = re.search(r'tt\d+', tmp_dict.get('url'))
        film_id = s_film_id.group()
        dict_film_basic['ttid'] = film_id
        dict_film_basic['name'] = tmp_dict.get('name')
        dict_film_basic['genre'] = tmp_dict.get('genre')
        if dict_film_basic['genre']:
            dict_film_basic['genre'] = str(dict_film_basic['genre'])
        dict_film_basic['contentRating'] = tmp_dict.get('contentRating')
        dict_film_basic['description'] = tmp_dict.get('description')
        dict_film_basic['datePublished'] = tmp_dict.get('datePublished')
        dict_film_basic['keywords'] = tmp_dict.get('keywords')
        agg_rating = tmp_dict.get('aggregateRating')
        if agg_rating:
            dict_film_basic['ratingCount'] = agg_rating.get('ratingCount')
            dict_film_basic['bestRating'] = agg_rating.get('bestRating')
            dict_film_basic['worstRating'] = agg_rating.get('worstRating')
            dict_film_basic['ratingValue'] = agg_rating.get('ratingValue')
        list_film_multi = []
        actors = tmp_dict.get('actor')
        if actors:
            for i in actors:
                tmp = {}
                tmp['ttid'] = film_id
                tmp['film_name'] = tmp_dict.get('name')
                tmp['type'] = 'actor'
                tmp['person_name'] = i.get('name')
                tmp['person_url'] = i.get('url')
                list_film_multi.append(tmp)
        directors = tmp_dict.get('director')
        if directors:
            if type(directors) == dict:
                directors = [directors]
            for i in directors:
                tmp = {}
                tmp['ttid'] = film_id
                tmp['film_name'] = tmp_dict.get('name')
                tmp['type'] = 'director'
                tmp['person_name'] = i.get('name')
                tmp['person_url'] = i.get('url')
                list_film_multi.append(tmp)
        creators = tmp_dict.get('creator')
        if creators:
            if type(creators) == dict:
                directors = [directors]
            for i in creators:
                tmp = {}
                tmp['ttid'] = film_id
                tmp['film_name'] = tmp_dict.get('name')
                tmp['type'] = 'creator'
                tmp['person_name'] = i.get('name')
                tmp['person_url'] = i.get('url')
                list_film_multi.append(tmp)
        dict_for_pandas = {'ttid':[], 'film_name':[], 'type':[], 'person_name':[], 'person_url':[]}
        for i in list_film_multi:
            for k in i.keys():
                dict_for_pandas[k].append(i.get(k))
        df_film_crew = pd.DataFrame(dict_for_pandas)
        return dict_film_basic, df_film_crew
    def scrape_title(self, title_url):
        title_url = urljoin(domain_url, title_url)
        if title_url in self.used_url_title:
            return False
        response = self.hd.request_proxy(title_url)
        if response:
            dict_film_basic, df_film_crew = self.parse_title(response.content)
            df_film_basic = self.save_title_basic(dict_film_basic)
            
            df_film_crew.to_sql(name=TABLENAME_FILM_CREW,con=self.conn,if_exists='append',index=False)
            self.used_url_add(title_url, self.used_url_title, FILEPATH_USEDURL_TITLE)
            return df_film_basic, df_film_crew
        else:
            self.used_url_add(title_url, self.error_url_title, FILEPATH_ERROR_TITLE)
            print('ERROR: {}'.format(title_url))
            return False
    def save_title_basic(self, dict_film_basic, to_db=True, table_name=TABLENAME_TITLE_BASIC):
        dict_for_pandas = {'ttid':[],'name':[], 'title_url':[], 'genre':[],
                          'contentRating':[],'datePublished':[],
                          'ratingCount':[], 'bestRating':[], 'worstRating':[], 'ratingValue':[],
                          'budget':[], 'open_weekend_usa':[], 'gross_usa':[], 'cumulative_worldwide_gross':[],
                          'description':[], 'keywords':[], 'plot_summary':[], 'storyline':[]}
        for k in dict_for_pandas.keys():
            dict_for_pandas[k].append(dict_film_basic.get(k))
        df_film_basic = pd.DataFrame(dict_for_pandas)
        if to_db:
            df_film_basic.to_sql(name=table_name,con=self.conn,if_exists='append',index=False)
        return df_film_basic
    def scrape_title_list(self, urllist, teststop=-1):
        for i in urllist:
            if teststop==0:
                print('test end')
                return
            if self.scrape_title(i):
                if teststop>0:
                    teststop = teststop-1
                time.sleep(1)
        print('mission complete')
            
# # scrape list          
# if __name__ == "__main__":
#     sp = imdb_spider()
#     sp.scrapy_li_tt_all()

# scrape title
if __name__ == "__main__":
    sp = imdb_spider()
    conn = sqlite3.connect(FILEPATH_DATABASE)
    cur = conn.cursor()
    df1 = pd.read_sql('select * from {}'.format(TABLENAME_FILMLIST), conn)
    
    def year_to_int(value):
        s_tmp = re.search(r'\d+',value)
        if s_tmp:
            tmp = s_tmp.group()
            tmp = np.int(tmp)
        else:
            tmp = np.nan
        return tmp
    df1['year'] = df1['year'].apply(year_to_int)
    df1 = df1[(df1['year']>2000) & (df1['year']<2019)]
    title_urls = df1['url'].values.tolist()
    sp.scrape_title_list(title_urls)
