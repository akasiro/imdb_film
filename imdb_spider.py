import requests,re,os,sqlite3,sys,time,json
from bs4 import BeautifulSoup
import pandas as pd
from imdb_config import *
from urllib.parse import urljoin
sys.path.append('/home/guijideanhao/pyproject/scrapy_toolv2')
from html_downloader import html_downloader

class imdb_spider():
    def __init__(self,dbpath=FILEPATH_DATABASE, hd=None):
        self.conn = sqlite3.connect(dbpath)
        self.cur = self.conn.cursor()
        if hd:
            self.hd = hd
        else:
            self.hd = html_downloader(china=False)
        self.filmlist_used_ttid()
        self.used_url_li_tt = self.used_url_gen(FILEPATH_USEDURL_LI_TT)
    
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
        dict_film_basic, df_film_crew = self.parse_title_json(script_data.get_text())
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
            
if __name__ == "__main__":
    sp = imdb_spider()
    sp.scrapy_li_tt_all()
