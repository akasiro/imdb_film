import requests,re,os,sqlite3,sys,time
from bs4 import BeautifulSoup
import pandas as pd
from imdb_config import *
from urllib.parse import urljoin
sys.path.append('/home/ubuntu/pyproject/scrapy_toolv2')
from html_downloader import html_downloader

class imdb_spider():
    def __init__(self,dbpath=FILEPATH_DATABASE, hd=None):
        self.conn = sqlite3.connect(dbpath)
        self.cur = self.conn.cursor()
        if hd:
            self.hd = hd
        else:
            self.hd = html_downloader(world=True)
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
            item_genre = i.find('span', {'class':'genre'})
            film_genre = item_genre.get_text()
            film_year = i.find('span', {'class':'lister-item-year'}).get_text()
        # modify
            s_film_id = re.search(r'tt\d+', film_url)
            film_id = s_film_id.group()
            film_genre = re.sub(r'\s','',film_genre)
            film_year = re.sub(r'\D', '', film_year)
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

            
if __name__ == "__main__":
    sp = imdb_spider()
    sp.scrapy_li_tt_all()
