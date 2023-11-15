import sqlite3
import pandas as pd
con = sqlite3.connect("store.sqlite")
f_damp = open('store.db','r', encoding ='utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)
con.commit()



df = pd.read_sql('''SELECT name_author AS "автор", name_genre AS "жанр", price AS "цена" 
FROM  book JOIN author USING (author_id) JOIN genre USING (genre_id)
WHERE price BETWEEN 300 AND 600 AND name_genre = 'Роман'
''', con)
print(df)
print()
print()



df = pd.read_sql('''SELECT DISTINCT name_client
FROM client JOIN buy USING (client_id)
JOIN buy_book  USING (buy_id)
WHERE amount > 1 ORDER BY name_client''', con)
print(df)
print()
print()



df = pd.read_sql('''
with totals as (
SELECT
    client_id,
    SUM(book.amount * price) AS total_price_of_order
FROM
    buy
    JOIN buy_book USING(buy_id)
    JOIN book USING(book_id)
)

SELECT title, name_author 
FROM book
JOIN author USING(author_id)
JOIN buy_book USING(book_id)
JOIN buy USING(buy_id)
JOIN totals USING(client_id)
WHERE total_price_of_order = (
    SELECT MAX(total_price_of_order) 
    FROM totals
)
''', con)
print(df)
print()
print()


cursor = con.cursor()
cursor.execute('''CREATE TABLE good_order AS SELECT a.name_author, b.title, b.price, (SELECT MAX(amount)-b.amount 
	FROM book WHERE author_id = b.author_id) AS 'Заказ' 
	FROM book b 
	INNER JOIN author a USING(author_id) WHERE (SELECT MAX(amount)-b.amount 
	FROM book WHERE author_id = b.author_id) > 0''')
con.commit()
df = pd.read_sql('''SELECT * FROM good_order''', con)
print(df)
print()
print()





df = pd.read_sql('''
SELECT name_author 
AS 'Автор', title AS 'Книга', price AS 'Стоимость', SUM(price) OVER(PARTITION BY name_author ORDER BY price) AS 'Стоимость_с_накоплением'
FROM author a
INNER JOIN book USING(author_id)
WHERE amount > 0
ORDER BY name_author DESC, price ASC
''', con)
print(df)
print()
print()




con.close()

