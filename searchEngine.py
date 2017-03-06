#coding:utf-8
import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import MySQLdb

# 构造一个单词列表，这些单词将被忽略
ignorewords=(['the','of','to','and','a','in','is','it'])
# 连接mysql数据库参数
connParams = dict(host='127.0.0.1',user='root',passwd='root',db='searchindex')
class crawler:
	# 初始化crawler类并传入数据库
	def __init__(self):
		self.con=MySQLdb.connect(**connParams)
		self.conn=self.con.cursor()
	def __del__(self):
		self.conn.close()
		self.con.close()
	def dbcommit(self):
		self.con.commit()

	# 辅助函数，用于获取条目的id，并且如果条目不存在，就将其加入到数据库中
	def getentryid(self,table,field,value,createnew=True):
		cur=self.conn.execute("select rowid from %s where %s='%s'" % (table,field,value))
		res=self.conn.fetchone()
		if res==None:
			cur=self.conn.execute("insert into %s (%s) values ('%s')" % (table,field,value))
			return self.conn.lastrowid
		else:
			return res[0]

	# 为每个网页建立索引
	def addtoindex(self,url,soup):
		if self.isindexed(url):
			return
		print('Indexing %s' % url)
		#获取每个单词
		text=self.gettextonly(soup)
		words=self.separatewords(text)
		#得到URL的id
		urlid=self.getentryid('urllist','url',url)
		#将每个单词与该url关联
		for i in range(len(words)):
			word=words[i]
			if word in ignorewords:
				continue
			wordid=self.getentryid('wordlist','word',word)
			self.conn.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid,wordid,i))
	# 从一个HTML网页中提取文字（不带标签的）
	def gettextonly(self,soup):
		v=soup.string

		if v==None:
			c=soup.contents

			resulttext=''
			for t in c:

				subtext=self.gettextonly(t)
				resulttext+=subtext+'\n'
			return resulttext
		else:
			return v.strip()

	# 根据任何费空白字符进行分词处理
	def separatewords(self,text):
		splitter=re.compile('\\W*')
		return [s.lower() for s in splitter.split(text) if s!='']

	# 如果url已经建立过索引，则返回ture
	def isindexed(self,url):
		# 对url中的%进行解码
		#url=urllib2.unquote(url)

		self.conn.execute("select rowid from urllist where url='%s'" % url)
		u=self.conn.fetchone()
		if u!=None:
			#检查它是否已经被检索过了
			self.conn.execute("select * from wordlocation where urlid=%d" % u[0])
			v=self.conn.fetchone()
			if v!=None:
				return True
		return False

	# 添加一个关联两个网页的链接
	def addlinkref(self,urlFrom,urlTo,linkText):
		pass

	# 从一小组网页开始进行广度优先搜索，直至某一给定深度，期间为网页建立索引
	def crawl(self,pages,depth=2):
		for i in range(depth):
			newpages=set()
			for page in pages:
				try:
					c=urllib2.urlopen(page)
					
				except:
					print("Could not open %s" % page)
					continue
				
				soup=BeautifulSoup(c.read())
	
				self.addtoindex(page,soup)

				links=soup('a')

				for link in links:
					
					if ('href' in dict(link.attrs)):
						url=urljoin(page,link['href'])

						if url.find("'")!=-1:
							continue
						url=url.split('#')[0] # 去掉位置部分
						#if '%' in url:
						#	url='0000'
						if url[0:4]=='http' and not self.isindexed(url):
							newpages.add(url)
						linkText=self.gettextonly(link)

						self.addlinkref(page,url,linkText)
				self.dbcommit()
			pages=newpages
	# 创建数据库表
	def createindextables(self):
		self.conn.execute('create table urllist(rowid int not null auto_increment primary key, url varchar(128) CHARACTER SET utf8)')
		self.conn.execute('create table wordlist(rowid int not null auto_increment primary key, word varchar(255) CHARACTER SET utf8)')
		self.conn.execute('create table wordlocation(rowid int not null auto_increment primary key,urlid int, wordid int,location int)')
		self.conn.execute('create table link(rowid int not null auto_increment primary key,fromid int,toid int)')
		self.conn.execute('create table linkwords(rowid int not null auto_increment primary key,wordid int,linkid int)')
		self.conn.execute('create index wordidx on wordlist(word)')
		self.conn.execute('create index urlidx on urllist(url)')
		self.conn.execute('create index wordurlidx on wordlocation(wordid)')
		self.conn.execute('create index urltoidx on link(toid)')
		self.conn.execute('create index urlfromidx on link(fromid)')
		self.dbcommit()
		self.__del__()



class searcher:
	def __init__(self):
		self.con=MySQLdb.connect(**connParams)
		self.conn=self.con.cursor()
	def __del__(self):
		self.conn.close()
		self.con.close()

	def getmatchrows(self,q):
		# 构造查询的字符串
		fieldlist='w0.urlid'
		tablelist=''
		clauselist=''
		wordids=[]

		# 根据空格拆分单词
		words=q.split(' ')
		tablenumber=0
		for word in words:
			# 获取单词的ID
			self.conn.execute("select rowid from wordlist where word='%s'" % word)
			wordrow=self.conn.fetchone()
			#wordrow=self.con.fetchone()
			if wordrow!=None:
				wordid=wordrow[0]
				wordids.append(wordid)
				
				if tablenumber>0:
					tablelist+=','
					clauselist+=' and '
					clauselist+='w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
				fieldlist+=',w%d.location' % tablenumber
				tablelist+='wordlocation w%d' % tablenumber
				clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
				tablenumber+=1

		# 根据各个组分，建立查询
		fullquery='select %s from %s where %s' % (fieldlist,tablelist,clauselist)
		print fullquery
		self.conn.execute(fullquery)
		cur=self.conn.fetchall()
		rows=[row for row in cur]
		#return wordids
		return rows,wordids
	def getscoredlist(self,rows,wordids):
		totalscores=dict([row[0],0] for row in rows)

		weights=[]

		for (weight,scores) in weights:
			for url in totalscores:
				totalscores[url]+=weight*scores[url]
		
		return totalscores

	def geturlname(self,id):
		self.conn.execute("select url from urllist where rowid=%d" % id)
		return self.conn.fetchone()

	def query(self,q):
		rows,wordids=self.getmatchrows(q)
		scores=self.getscoredlist(rows,wordids)

		rankescores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)

		for (score,urlid) in rankescores[0:10]:
			print '%f\t%s' % (score,self.geturlname(urlid))
	def normalizescores(self,scores,smallIsBetter=0):
		vsmall=0.00001 # 避免被零整除
		if smallIsBetter:
			minscore=min(scores.values())
			return dict([(u,float(minscore)/max(vsmall,1)) for (u,l) in scores.items()])
		else:
			maxscore=max(scores.values())
			if maxscore==0:
				maxscore=vsmall
			return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])
		def frequencyscore(self,rows):
			counts=dict([(row[0],0) for row in rows])
			for row in rows:
				counts[row[0]]+=1
			return self.normalizescores(counts)
'''
#pages=['http://www.bbc.com']
crawler=crawler()
#crawler.crawl(pages)
a=crawler.conn.execute('select location from wordlocation where urlid=3')
b=crawler.conn.fetchall()
print [row for row in b]
'''
'''
e=searcher()
print e.query('Design implementation')
'''