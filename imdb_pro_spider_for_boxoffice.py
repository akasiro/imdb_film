import requests,re,os,sqlite3,sys,time,json
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import pandas as pd
import numpy as np
from imdb_pro_config import *
from urllib.parse import urljoin
sys.path.append('/home/guijideanhao/pyproject/scrapy_toolv2')
from html_downloader import html_downloader
from log_manager import log_manager

class imdb_pro_spider():
    '''
    spider for https://pro.imdb.com/
    
    Attributes:
        cookies (dict): cookies
        headers (dict): headers
        session (object): requests Session() object
        log_manager (object)
        success_ttid (list)
        error_ttid (list)
        table_name (str): the table save box office
        db_path (str): the path of database
        conn (object): the sqlite3 connection object
    '''
    def __init__(self, cookies_file, headers_file,table_name, db_path, log_filepath='imdb_pro.txt'):
        '''
        Args:
            cookies_file (str): full cookies_file path in txt format
            headers_file (str): full headers_file path in txt format
            table_name (str): the table save box office
            db_path (str): the path of database
            log_filepath (str): full path of log file
        '''
        self.cookies = self.gen_cookies(cookies_file)
        self.headers_file = headers_file
        self.session = requests.Session()
        self.log_manager = log_manager(log_filepath)
        # replicate list
        self.success_ttid = self.log_manager.get_info_list(success_tag='SUCCESS')
        self.error_ttid = self.log_manager.get_info_list(success_tag='ERROR')
        
        self.table_name = table_name
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        
    def gen_cookies(self, cookies_file):
        '''
        Args:
            cookies_file (str): full cookies_file path in txt format
        
        Returns:
            dict: cookies dict
        '''
        cookies = {}
        with open(cookies_file, 'r') as f:
            cookies_str = f.read()
        list_cookies = cookies_str.split(';')

        for i in list_cookies:
            name,value=i.strip().split('=',1)
            cookies[name] = value
        return cookies
    def gen_headers(self, headers_file, ttid):
        '''
        Args:
            headers_file (str): full headers_file path in txt format
        
        Returns:
            dict: headers dict
        '''
        headers = {}
        with open(headers_file, 'r') as f:
            for i in f.readlines():
                name,value = i.strip().split(':',1)
                headers[name] = value.strip()
        headers['Referer'] = 'https://pro.imdb.com/title/{}/boxoffice'.format(ttid)
        return headers
    def download_page(self, ttid):
        '''
        download html page
        
        Args:
            ttid (str): film ttid
        
        Returns:
            None or Response object: if fail return None, else response object

        '''
        url = 'https://pro.imdb.com/title/{}/boxoffice/_ajax'.format(ttid)
        headers = self.gen_headers(self.headers_file,ttid)
        res = self.session.get(url=url, cookies=self.cookies, headers=headers)
        if res.status_code == 200:
            return res
        else:
            return None
    def parse_page_boxoffice(self, res_content, ttid):
        '''
        parse page to gain box office data
        
        Args:
            res_content (binary): web page binary
            ttid (str): film ttid
        
        Returns:
            bool: True if successfully gain data
            df: if gain data return data
            
        to do:
            * add a empty list
            * add a used list
            * add an error list
        '''
        soup = BeautifulSoup(res_content, 'html.parser')
        empty_table = soup.find('span', {'class':'empty_mojo_table'})
        box_office_mojo = soup.find('table', {'id':'box_office_mojo'})
        if empty_table:
            self.log_manager.write_log(info=ttid,info_type='no data',add_to_list=self.success_ttid)
            return True,
        elif not box_office_mojo:
            # todo
            # add a list of error print add to list function
            print('ERROR: {}'.format(ttid))
            self.log_manager.write_log(info=ttid,info_type='log',success=False)
            return False,
        else:
            df = self.parse_table_boxoffice(box_office_mojo, ttid)
            # todo
            # add a list of used ttid print congratulation
            return True, df
            
    def parse_table_boxoffice(self, box_office_mojo, ttid):
        '''
        Transform the table in the website to dataframe
        
        Args:
            box_office_mojo (BeautifulSoup object): table object
            
        Returns:
            dataframe
        '''
        dict_for_pandas = {'date1':[], 'date2':[], 'date3':[], 'date4':[],
                           'single_day_gross':[], 'single_day_gross_rank':[],
                          'change_yesterday':[], 'change_last_week':[],
                          'theaters':[], 'avg_per_theaters':[], 'gross_since_release':[]}
        list_tmp_dict = []
        trs = box_office_mojo.find_all('tr')
        for i in trs:
            tmp_dict = {}
            if i.has_attr('class'):
                if 'heading' in i['class']:
                    continue
            tds = i.find_all('td')
            # element 0 dates
            tmp_dict['date1'] = tds[0].get('data-sort-value')
            dates = tds[0].div.find_all('p', {'class':'a-spacing-mini'})
            tmp_dict['date2'] = dates[0].get_text()
            tmp_dict['date3'] = dates[1].get_text()
            if i.has_attr('class'):
                if 'box_office_mojo_special_occasion_row' in i.get('class'):
                    tmp_dict['date4'] = dates[2].get_text()
            # element 1 single day gross ($)
            tmp_dict['single_day_gross'] = tds[1].get('data-sort-value')
            sdgs = tds[1].div.find_all('p', {'class':'a-spacing-mini'})
            tmp_dict['single_day_gross_rank'] = sdgs[0].get_text()
            # element 2 change yesterday (%)
            tmp_dict['change_yesterday'] = tds[2].get('data-sort-value')
            # element 3 change_last_week (%)
            tmp_dict['change_last_week'] = tds[3].get('data-sort-value')
            # element 4 theaters
            tmp_dict['theaters'] = tds[4].get('data-sort-value')
            # element 5 avg_per_theaters ($)
            tmp_dict['avg_per_theaters'] = tds[5].get('data-sort-value')
            # element 6 gross_since_release ($)
            tmp_dict['gross_since_release'] = tds[6].get('data-sort-value')
            
            list_tmp_dict.append(tmp_dict)
        for k in dict_for_pandas.keys():
            for l in list_tmp_dict:
                dict_for_pandas[k].append(l.get(k))
        df = pd.DataFrame(dict_for_pandas)
        rearrange_col = ['ttid'] + df.columns.tolist()
        df['ttid'] = ttid
        df = df[rearrange_col]
        return df
    def parse_save_ttid_list(self, list_ttid, teststop=-1):
        '''
        parse and save
        Args:
            list_ttid (list)
            teststop (int): for text
        '''
        for i in list_ttid:
            if teststop == 0:
                print('test end')
                break
            # replicate check
            if i in self.success_ttid or i in self.error_ttid:
                continue
            # download page
            try:
                res = self.download_page(i)
                if res == None:
                    self.log_manager.write_log(success=False, info_type='server forbid', info=i, add_to_list=self.error_ttid)
                    break
            except:
                self.log_manager.write_log(success=False,info_type='download unknown', info=i, add_to_list=self.error_ttid)
                break
            # parse page
            try:
                success,*df = self.parse_page_boxoffice(res.content, i)
                if success:
                    if df == []:
                        time.sleep(60)
                        continue
                else:
                    break
            except:
                self.log_manager.write_log(info=i, success=False, info_type='parse_unknown',add_to_list=self.error_ttid)
                continue
                
            # save df
            try:
                df[0].to_sql(name=self.table_name,con=self.conn,if_exists='append',index=False)
                self.log_manager.write_log(info=i, add_to_list=self.success_ttid)
                if teststop>0:
                    teststop = teststop -1
                time.sleep(60)
            except:
                self.log_manager.write_log(info=i, success=False, info_type='db', add_to_list=self.error_ttid)
                time.sleep(60)
                continue
        print('mission complete')
                
if __name__ == '__main__':
    conn = sqlite3.connect('/home/guijideanhao/pyproject/imdb/imdb_data/imdb_film.db')

    df1 = pd.read_sql('select * from {}'.format('film_list'), conn)

    def year_to_int(value):
        s_tmp = re.search(r'\d+',value)
        if s_tmp:
            tmp = s_tmp.group()
            tmp = np.int(tmp)
        else:
            tmp = np.nan
        return tmp
    df1['year'] = df1['year'].apply(year_to_int)
    df1 = df1[df1['year']==2018]
    ttids = df1['ttid'].values.tolist()
    
    test = imdb_pro_spider(cookies_file=FILE_COOKIES,headers_file=FILE_HEADERS,table_name=TABLE_NAME_BOXOFFICE,db_path=FILE_DABABASE)
    test.parse_save_ttid_list(ttids)
