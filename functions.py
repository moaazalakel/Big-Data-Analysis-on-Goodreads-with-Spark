#Functions
#1: Most genre have books
#use this function and do map and reduce(wordcount)
def get_AllGenres(book_metadata):
    """
    this function output every genre assigned to every book in the book_metadata goodreads_books.json.gz
    """
    genre_list=[]
    book_dict=json.loads(book_metadata)
    if len(book_dict['popular_shelves']) != 0:
        for genre in  book_dict['popular_shelves']:
            #it is a list of genres
            genre_list.append(genre['name'])
    return genre_list


#2 most language for every genre

# use only this function as it should return for every book a list of genres assiciated with the language of the book
def get_Lang_with_Genres(book_metadata):
    """
    this function output every genre assigned to every book in the book_metadata goodreads_books.json.gz
    """
    genre_list=[]
    lang_genre_list=[]
    book_dict=json.loads(book_metadata)
    book_lang=book_dict['language_code']
    if len(book_dict['popular_shelves']) != 0:
        for genre in  book_dict['popular_shelves']:
            #it is a list of genres
            genre_list.append(genre['name'])
    if len(book_lang)!=0:
        lang_genre_list=[book_lang+'***'+i for i in genre_list]

    return lang_genre_list


#3-Most number of words for one writer
# this process will be on two Functions
# the first function is the most important as it will just use the writer id
# we can transform the writer id onto a writer name if we have time

words=books_data.flatMap(lambda line:myfunction(line))
wordcount=words.map(lambda key:(key,1)).reduceByKey(lambda key,value: key+value)
data2=authors_data.map(lambda line:get_authorName(ID))
data2.join(wordcount)
out=data2.sortBy(lambda x: x[1][1], ascending=False)
result = out.collect()

def get_authorID(book_metadata):
    """
    this function output every author assigned to every book in the book_metadata goodreads_books.json.gz
    """
    authorsID_list=[]
    book_dict=json.loads(book_metadata)
    authors_list=book_dict['authors']
    if len(authors_list) != 0:
        for author in  authors_list:
            #it is a list of genres
            authorsID_list.append(author['author_id'])
    return authorsID_list
def get_authorName(author_data):
    author_dict=json.loads(author_data)
    authorsID_names_list=[]
    id=author_dict['author_id']
    name=author_dict['name']
    authorsID_names_list.append(id+'***',name)
    #return authorsID_names_list
    return (id,name)



#4-Most books have reviews
# this Process will be on two functions also
# the first function is the important but it gives the number of reviews for a book id
# later we can transform the book id onto a book name

def get_bookID_reviews(review):
    """
    this function output every genre assigned to every book , using the reviews file : goodread_reviews_dedup.json.gz
    """
    BookID_list=[]
    review_dict=json.loads(review)
    book_id=review_dict['book_id']
    if len(book_id) != 0:
        BookID_list.append(book_id)
    return BookID_list
# I found that i can get the reviews number from the book metadata:
# but also use the same map function as mentinoed in requirement number 6
def get_books_reviews(book_metadata):
    """
    this function output number of reviews for every book in the book_metadata goodreads_books.json.gz
    """
    reviews_list=[]
    book_dict=json.loads(book_metadata)
    book_name=book_dict['title_without_series']
    reviews_num=book_dict['text_reviews_count']
    if len(reviews_num) != 0:
            reviews_list.append(book_name+'*-_-*'+reviews_num)
    return reviews_list


#5-Most readed books
# first of all I am assuming that spark is reading a csv file line-by-line as a text
# i also wrote another code if spark read csv like json (indexing with the column name not buy a number)
# this requirement will take 3 processess
# the first process is the important one as it will return the scv id of the book with the number of reads for it
# the second process should transform the csv id into regular book id then the third will transfrom the book id onto book name


def get_read_books_csvID(interaction):
    """
    this will get the readerd books only , it should be used with the interactions file: goodreads_interactions.csv
    """
    booksIDS_list=[]
    interactiondata=interaction.split(',')
    # is read is the third element
    is_read=int(int(interactiondata[2]))
    # book id is the second element
    book_id=interactiondata[1]
    if is_read ==1:
        booksIDS_list.append(book_id)
    return genre_list

def get_read_books_csvID(interaction):
    """
    this will get the readerd books only , it should be used with the interactions file: goodreads_interactions.csv
    """
    booksIDS_list=[]
    interactiondata=json.loads(interaction)
    # is read is the third element
    is_read=interactiondata['is_read']
    # book id is the second element
    book_id=interactiondata['book_id']
    if is_read ==True:
        booksIDS_list.append(book_id)
    return genre_list




# 6-Most “want to read” books
# this will be on one process only
# i will get the count of the 'to-read' shelf in popular shelves for every book book_metadata
#this function should return
# remember this funciton will not use code for map:>>>>> words.map(lambda key:(key,1))
# change map to code:>>>>>>>>     words.map(lambda key:(key.split('*-_-*')[0],int(key.split('*-_-*')[1])))

def get_AllGenres(book_metadata):
    """
    this functoin gets the number of to-reads for every book in the book_metadata goodreads_books.json.gz
    """
    books_with_toReadNum_list=[]
    book_dict=json.loads(book_metadata)
    book_name=book_dict['title_without_series']
    if len(book_dict['popular_shelves']) != 0:
        for genre in  book_dict['popular_shelves']:
            #it is a list of genres
            if genre['name']== 'to-read'
                books_with_toReadNum_list.append(book_name+'*-_-*'+genre['count'])
    return books_with_toReadNum_list


#8-Most year have published books


def get_year(book_metadata):
    """
    this function output every year assigned to every book in the book_metadata goodreads_books.json.gz
    """
    year_list=[]
    book_dict=json.loads(book_metadata)
    year=book_dict['publication_year']
    if len(year) != 0:
        year_list.append(year)
    return year_list






#10-Most double genre for books(children&Fantasy, romance&thriller,etc.). 2 genres, 3 genres
#There are 2 function , the first to get a comination of every 2 genres for every books
# the second function is not important
from itertools import combinations
def get_2genre_combination(book_fuzzy):
    """
    this function output every 2genre associated for every book in the book_metadata gooreads_book_genres_initial.json.gz
    """
    genre_syn=[]
    genre_list=[]
    #avoid_list=['to-read','owned','currently-reading','favorites','books-i-own','kindle','ebook','library','default','owned-books','to-buy','ebooks','wish-list','my-books','my-library','e-book','audiobook','books','i-own','audiobooks','audio','read-in-2016','favourites','e-books','read-in-2015','read-in-2017','own-it','maybe','read-in-2014','abandoned','did-not-finish'] # add others if found
    #lookup_list=['fiction','romance','adult','non-fiction','fantasy','mystery','novels','nonfiction','history','historical','young-adult','historical-fiction','adult-fiction','adventure','literature','thriller','classics','suspense','paranormal','science-fiction','crime','drama','sci-fi','humor','contemporary-romance','general-fiction','contemporary-fiction','family','school','mystery-thriller','childrens','short-stories','children','biography','classic','literary-fiction','sci-fi-fantasy','horror','supernatural','mysteries','magic','american','children-s','action','kids','mystery-suspense','british','netgalley','science','children-s-books','stand-alone','philosophy','erotica','teen','urban-fantasy','funny','religion','fantasy-sci-fi','realistic-fiction','literary','scifi','politics','memoir','love','war','childrens-books','paranormal-romance','thrillers','humour','europe','friendship','childhood','scifi-fantasy','comics','psychology','short-story','kids-books','women','detective','graphic-novels','action-adventure','crime-fiction','middle-grade']
    book_dict=json.loads(book_fuzzy)
    if len(book_dict['genres']) >=2 :
        for genre in  book_dict['genres']:
            #it is a list of genres
            genre_list.append(genre)
        #genre_list_mod=[i for i in genre_list if i not in avoid_list]
        genre_combination=combinations(genre_list,2)
        for combination in genre_combination:
            genre_syn.append('***'.join(list(sorted(combination))))

    return genre_syn



#11-Most Writer with genre
# this funciton will use the book metadata and will get every author with every genre in the book
from itertools import product
def get_writerID_with_genre(book_metadata):
    """
    this function output every genre assigned to authors of every book in the book_metadata goodreads_books.json.gz
    """
    writer_genre_list=[]
    #avoid_list=['to-read','owned','currently-reading','favorites','books-i-own','kindle','ebook','library','default','owned-books','to-buy','ebooks','wish-list','my-books','my-library','e-book','audiobook','books','i-own','audiobooks','audio','read-in-2016','favourites','e-books','read-in-2015','read-in-2017','own-it','maybe','read-in-2014','abandoned','did-not-finish'] # add others if found
    lookup_list=['fiction','romance','adult','non-fiction','fantasy','mystery','novels','nonfiction','history','historical','young-adult','historical-fiction','adult-fiction','adventure','literature','thriller','classics','suspense','paranormal','science-fiction','crime','drama','sci-fi','humor','contemporary-romance','general-fiction','contemporary-fiction','family','school','mystery-thriller','childrens','short-stories','children','biography','classic','literary-fiction','sci-fi-fantasy','horror','supernatural','mysteries','magic','american','children-s','action','kids','mystery-suspense','british','netgalley','science','children-s-books','stand-alone','philosophy','erotica','teen','urban-fantasy','funny','religion','fantasy-sci-fi','realistic-fiction','literary','scifi','politics','memoir','love','war','childrens-books','paranormal-romance','thrillers','humour','europe','friendship','childhood','scifi-fantasy','comics','psychology','short-story','kids-books','women','detective','graphic-novels','action-adventure','crime-fiction','middle-grade']
    genre_list=[]
    authors_list=[]
    book_dict=json.loads(book_metadata)
    if len(book_dict['popular_shelves']) != 0:
        for genre in  book_dict['popular_shelves']:
            #it is a list of genres
            if genre['name'] in lookup_list:
                genre_list.append(genre['name'])
    if len(book_dict['authors'])!=0 and len(genre_list)!=0:
        for author in book_dict['authors']:
            authors_list.append(author['author_id'])
        writer_genre_list=list(product(authors_list,genre_list))

    return writer_genre_list
