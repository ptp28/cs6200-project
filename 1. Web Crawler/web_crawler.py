from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import requests
import heapq
import time
import math
import json
import ast

def getResponse(url):
    # Fetch the webpage content
    response = requests.get(url, timeout=2)
    return response

# Check if the page is valid and in English
def validPage(response):
    headers = response.headers
    lang = headers["content-language"] if "content-language" in headers else "en"
    contentType = headers["content-type"].split(";") if "content-type" in headers else "text/html"

    return lang == "en" and contentType[0] and contentType[0] == "text/html"

def getHtmlContent(response):
    htmlContent = response.text
    # Parse the HTML content
    return BeautifulSoup(htmlContent, 'html.parser')

def getTitle(soup):
    title = soup.find('title')
    return title.text if title else ""

def getText(soup):
    text = ""
    # Find all elements of p tag
    paras = soup.find_all('p')

    for para in paras:
        text += para.text

    return text

def getFileContent(url, title, text):
    return f"<DOC>\n<DOCNO>{url}<\\DOCNO>\n<HEAD>{title}<\\HEAD>\n<TEXT>{text}<\\TEXT><\\DOC>\n"

def writePqIntoFile(fileName, content):
    with open(fileName, "w",encoding='utf-8') as f:
        for item in content:
            f.write(str(item)+"\n")

def writeIntoFile(fileName, content):
    with open(fileName, "a",encoding='utf-8') as f:
        f.write(content)

def verify_robot_parse(url):
    robot_parser=RobotFileParser()

    robot_parser.set_url(f"{url}+/robots.txt")

    robot_parser.read()
    user_agent="*"

    return robot_parser.can_fetch(user_agent, url)

def canonicalizeURL(baseUrl, url):
    ## Parses the URL
    parsedUrl = urlparse(url)

    ## Converts to lowercase
    scheme = parsedUrl.scheme.lower()
    netloc = parsedUrl.netloc.lower()

    ## Remove port
    if parsedUrl.port:
        netloc = parsedUrl.hostname

    ## Remove the fragment
    fragment = ""

    ## Remove duplicate //
    path = parsedUrl.path.replace('//', '/')

    ## Get absolute path
    if not scheme:
        parsedBaseUrl = urlparse(baseUrl)
        scheme = parsedBaseUrl.scheme.lower()
        netloc = parsedBaseUrl.netloc.lower()
        path = url

    return urlunparse((scheme, netloc, path, parsedUrl.params, parsedUrl.query, fragment))

def traverseLinks(baseUrl, soup, waveNumber,inlinks,outlinks,pq,inlinksCounter):
    # Find all elements of anchor tag
    links = soup.find_all('a', href=True)

    if baseUrl not in outlinks:
        outlinks[baseUrl] = set()

    for link in links:
        hrefLink = link.get("href")
        cl = canonicalizeURL(baseUrl, hrefLink)

        if cl not in inlinks:
            inlinks[cl] = set()
            inlinksCounter[cl] = 0

        ## from baseUrl, link is going out
        outlinks[baseUrl].add(cl)

        ## from link, baseUrl is coming in
        inlinks[cl].add(baseUrl)
        inlinksCounter[cl] += 1

    if pq.getSize() < 30000:
        for cl in inlinksCounter:
            pq.push(cl, inlinksCounter[cl], waveNumber + 1)

class PriorityQueue:
    def __init__(self):
        self.KEYWORDS=[
            "Alaska",
            "Purchase",
            "Klondike",
            "Gold",
            "Yukon",
            "Rush",
            "Indigenous",
            "Russian",
            "America",
            "Statehood",
            "Denali",
            "Eskimo",
            "Arctic",
            "Permafrost",
            "Tundra",
            "Fishing",
            "Explorers",  
            "Kodiak",
            "Oil",
            "Tourism",
            "Tribes",
            "Mining",
            "Mine",
            "Seward",
            "Settlement"
        ]
        self._queue = []

    # min heap is maintained by default
    def push(self, link, waveNumber, inLinkCount):
        keyword_hits=0
        for keyword in self.KEYWORDS:
            if keyword in link:
                keyword_hits+=1
        keyword_score=math.exp(-keyword_hits)
        inlink_score=math.exp(-inLinkCount)
        curr_score=keyword_score+inlink_score
        heapq.heappush(self._queue, (waveNumber, curr_score, link))

    def pop(self):
        waveNumber,inLinkCount, link = heapq.heappop(self._queue)
        return waveNumber, link

    def getSize(self):
        return len(self._queue)

    def loadQueue(self, queue):
        self._queue = queue

    def getQueue(self):
        return self._queue
    
def writeLinksToFile(linksCount,thread_id,inlinks,outlinks):
    inlinks_data={key : list(value) for key,value in inlinks.items()}
    outlinks_data={key : list(value) for key,value in outlinks.items()}
    with open(f"./crawler_op/links/inlinks_{linksCount}_thread_{thread_id}.json",'w') as inlinks_file:
        json.dump(inlinks_data,inlinks_file,indent=2)
    with open(f"./crawler_op/links/outlinks_{linksCount}_thread_{thread_id}.json",'w') as outlinks_file:
        json.dump(outlinks_data,outlinks_file,indent=2)

def getPq(filename):
    # Open the file in read mode
    with open(filename, 'r') as file:
        # Read all lines into a list
        contentPq = file.readlines()

    pq = PriorityQueue()

    def loadPq(contentPq):
        queue = []

        for content in contentPq:
            queue.append(ast.literal_eval(content))

        pq.loadQueue(queue)

    loadPq(contentPq)

    return pq

def getInlinks(filename):
    with open(filename, 'r') as file:
        inlinks = json.load(file)

    inlinksCounter = {}

    for link in inlinks:
        inlinksCounter[link] = len(inlinks[link])

    for link in inlinks:
        inlinks[link] = set(inlinks[link])

    return inlinks, inlinksCounter

def getOutLinks(filename):
    with open(filename, 'r') as file:
        outlinks = json.load(file)

    for link in outlinks:
        outlinks[link] = set(outlinks[link])

    return outlinks

visited_urls= {}
lock = Lock()

def crawl_seed_URL(baseUrl, thread_id):
    global visited_urls

    if baseUrl:
        print("Base url: ", baseUrl)
        inlinks = {}
        inlinksCounter = {}
        outlinks = {}

        pq = PriorityQueue()
        pq.push(baseUrl, 0, 0)
    else:
        inlinks = inlinks
        inlinksCounter = inlinksCounter
        outlinks = outlinks

        pq = pq


    now = time.time()

    linksCount = 1
    data=""
    file_id=1


    while linksCount <= 10000:
        time.sleep(1)
        waveNumber, currentUrl = pq.pop()

        if currentUrl not in visited_urls:
            # To verify the links and create more seeds to improve speed
            visited_urls[currentUrl] = 1
        else:
            continue

        parsedUrl = urlparse(currentUrl)
        hostname = parsedUrl.netloc.lower().split('.')

        try:
            if linksCount % 1000 == 0:
              print(f"On {linksCount} for thread {thread_id}")

            response = getResponse(currentUrl)

            ## if current page is non-html or not in english or does not match robots.txt requirements
            if not validPage(response) or not verify_robot_parse(currentUrl):
                continue

            soup = getHtmlContent(response)

            title = getTitle(soup)
            text = getText(soup)

            print("Title: " + title)
            
            traverseLinks(currentUrl, soup, waveNumber,inlinks,outlinks,pq,inlinksCounter)
            data+=getFileContent(currentUrl,title,text)

            if linksCount % 500 == 0:
                print(f"Writing files to webpage_data_{file_id} by thread id :{thread_id}.txt")
                writeIntoFile(f"./crawler_op/webpage/webpage_data_{file_id}_thread_{thread_id}.txt", data)
                file_id+=1
                data=""

                writePqIntoFile(f"./crawler_op/pq/priority_queue{linksCount}_thread_{thread_id}.txt", pq.getQueue())
                writeLinksToFile(linksCount,thread_id,inlinks,outlinks)
                

            linksCount += 1
        except Exception as e:
            pass

    print(f'Time: {time.time() - now} seconds')

"""
Each seed will initiate a different thread, and file_id is the id of the file where the results will be stored, set the file_id unique enough so that multiple threads don't write onto the same file
"""
seed_list=[
    "https://en.wikipedia.org/wiki/History_of_Alaska",
    "https://en.wikipedia.org/wiki/Alaska",
    "https://en.wikipedia.org/wiki/Klondike_Gold_Rush",
    "https://en.wikipedia.org/wiki/Alaska_Purchase",
    "https://en.wikipedia.org/wiki/Alaska_Purchase",
]

with ThreadPoolExecutor(max_workers=5) as thread_executor:
    for thread_id,baseUrl in enumerate(seed_list):
        thread_executor.submit(crawl_seed_URL,baseUrl,thread_id+1)