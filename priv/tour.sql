-- transaction_db のひととおりの機能をなぞる。
-- sql_shell:tour(). または bin/sqlsh --tour で実行する。

-- ============ DDL ============
CREATE TABLE fruit (name VARCHAR, price INTEGER, ripe BOOLEAN);

-- 同じ名前は作れない
CREATE TABLE fruit (name VARCHAR);

-- ============ DMLにはトランザクションが要る ============
INSERT INTO fruit VALUES ('apple', 100, true);

BEGIN;

-- ============ INSERT ============
INSERT INTO fruit VALUES ('apple', 100, true);
INSERT INTO fruit VALUES ('banana', 150, false);
INSERT INTO fruit VALUES ('orange', 150, true);

-- カラムを指定すると宣言順でなくてよい。指定しなかったカラムはNULL
INSERT INTO fruit (price, name) VALUES (300, 'grape');

-- 型が合わなければ弾かれる
INSERT INTO fruit VALUES ('bad', 'not a number', true);

-- 値の個数が合わなければ弾かれる
INSERT INTO fruit VALUES ('short', 1);

-- ============ SELECT ============
SELECT * FROM fruit;

SELECT name, price FROM fruit;

SELECT * FROM fruit WHERE price = 150;

-- 文字列リテラルで引ける(VARCHARと宣言してあるため)
SELECT * FROM fruit WHERE name = 'apple';

-- キーワードも識別子も大文字小文字を区別しない
select NAME from FRUIT where PRICE = 300;

-- NULLは比較でNULLになり、WHEREを通らない(3値論理)
SELECT * FROM fruit WHERE ripe = true;

-- ============ UPDATE ============
UPDATE fruit SET price = 120 WHERE name = 'apple';
SELECT * FROM fruit WHERE name = 'apple';

-- WHEREなしは全件。複数カラムの代入もできる
UPDATE fruit SET price = -1, ripe = false;
SELECT * FROM fruit;

-- ============ DELETE ============
DELETE FROM fruit WHERE name = 'banana';
SELECT * FROM fruit;

-- ============ コミット ============
COMMIT;

-- ============ ロールバック ============
BEGIN;
INSERT INTO fruit VALUES ('melon', 400, true);
SELECT * FROM fruit;
ROLLBACK;

BEGIN;
SELECT * FROM fruit;
COMMIT;

-- ============ エラーの出方 ============
BEGIN;
SELECT * FROM nosuchtable;
SELECT nosuchcolumn FROM fruit;
SELECT FROM;
COMMIT;

-- ============ 比較・論理・算術 ============
BEGIN;
SELECT * FROM fruit WHERE price > 100;
SELECT * FROM fruit WHERE price >= 100 AND price < 300;
SELECT * FROM fruit WHERE price < 100 OR price > 200;
SELECT * FROM fruit WHERE NOT price = 100;
SELECT * FROM fruit WHERE (price = 100 OR price = 150) AND ripe = true;

-- NULLは比較でNULLになり、NOTをつけても通らない(3値論理)
SELECT * FROM fruit WHERE ripe IS NULL;
SELECT * FROM fruit WHERE ripe IS NOT NULL;

-- 射影にも式が書ける
SELECT name, price * 2 FROM fruit;

-- 代入の右辺は更新前の行に対して評価する
UPDATE fruit SET price = price + 10;
SELECT * FROM fruit;
COMMIT;

-- ============ 並べ替えと件数制限 ============
BEGIN;
SELECT * FROM fruit ORDER BY price;
SELECT * FROM fruit ORDER BY price DESC;

-- NULLの位置は既定でASCならlast、DESCならfirst。明示もできる
SELECT * FROM fruit ORDER BY ripe ASC NULLS FIRST;

SELECT * FROM fruit ORDER BY price LIMIT 2;
SELECT * FROM fruit ORDER BY price LIMIT 2 OFFSET 1;

-- 並べ替えは射影の前なので、出力に無い列でも並べ替えられる
SELECT name FROM fruit ORDER BY price DESC;

SELECT DISTINCT price FROM fruit ORDER BY price;
COMMIT;

-- ============ 集約 ============
BEGIN;
SELECT COUNT(*) FROM fruit;

-- COUNT(*) は NULL の行も数えるが、COUNT(x) は数えない
SELECT COUNT(*), COUNT(ripe) FROM fruit;

SELECT SUM(price), AVG(price), MIN(price), MAX(price) FROM fruit;
SELECT ripe, COUNT(*) FROM fruit GROUP BY ripe;
SELECT ripe, SUM(price) FROM fruit GROUP BY ripe HAVING COUNT(*) > 1;
SELECT COUNT(DISTINCT ripe) FROM fruit;

-- GROUP BY に無く集約でもないカラムは、どの行の値か決まらないので弾かれる
SELECT name FROM fruit GROUP BY ripe;
COMMIT;

-- ============ JOIN ============
-- DDLはトランザクションの外で
CREATE TABLE box (fruit VARCHAR, qty INTEGER);

BEGIN;
INSERT INTO box VALUES ('apple', 5);
INSERT INTO box VALUES ('banana', 3);
INSERT INTO box VALUES ('durian', 1);

SELECT f.name, b.qty FROM fruit f JOIN box b ON f.name = b.fruit;

-- 一致しない左の行はNULLで埋めて残る
SELECT f.name, b.qty FROM fruit f LEFT JOIN box b ON f.name = b.fruit;

-- カンマ区切りは直積
SELECT f.name, b.fruit FROM fruit f, box b;

-- 結合の上に WHERE / GROUP BY / ORDER BY を重ねられる
SELECT f.name, b.qty FROM fruit f JOIN box b ON f.name = b.fruit WHERE b.qty > 2;
SELECT COUNT(*) FROM fruit f JOIN box b ON f.name = b.fruit;

-- 修飾しないと決まらない名前は弾かれる
SELECT name FROM fruit f JOIN box b ON f.name = b.fruit;
COMMIT;

DROP TABLE box;

-- ============ 実行計画 ============
-- EXPLAIN は実行せずに、選ばれた計画を返す
EXPLAIN SELECT name FROM fruit WHERE price >= 150;

-- 述語は結合の下へ落ちる。結合してから捨てるより、捨ててから結合する方が
-- 中間結果が小さい
CREATE TABLE box (fruit VARCHAR, qty INTEGER);
EXPLAIN SELECT f.name FROM fruit f JOIN box b ON f.name = b.fruit
  WHERE f.price > 100 AND b.qty > 0;

-- ============ 索引 ============
-- 索引は明示的に作る。CREATE TABLE は自動では張らない
CREATE INDEX fruit_price ON fruit (price);
-- 対話シェルなら \di で索引の一覧が見られる

-- 統計を採ると、索引を使うかどうかを費用で判断できるようになる
ANALYZE fruit;
EXPLAIN SELECT name FROM fruit WHERE price = 150;

-- 索引の無いカラムでも引ける(全表走査に落ちる)
BEGIN;
SELECT name FROM fruit WHERE name = 'apple';
COMMIT;

-- 同じ索引名は作れない / 同じカラムに二重には張れない
CREATE INDEX fruit_price ON fruit (name);
CREATE INDEX fruit_price2 ON fruit (price);
DROP INDEX fruit_price;
DROP INDEX fruit_price;

DROP TABLE box;

-- ============ 副問い合わせ ============
BEGIN READ ONLY;
-- スカラー副問い合わせ
SELECT name FROM fruit WHERE price = (SELECT MAX(price) FROM fruit);
-- IN / NOT IN。値の並びでも副問い合わせでもよい
SELECT name FROM fruit WHERE price IN (100, 150);
-- 候補に NULL があると NOT IN は決して真にならない(3値論理)
SELECT name FROM fruit WHERE price NOT IN (100, null);
-- 1行1列でなければエラー
SELECT name FROM fruit WHERE price = (SELECT price FROM fruit);
COMMIT;

-- 導出表。別名は必須
BEGIN READ ONLY;
SELECT d.name FROM (SELECT name, price FROM fruit WHERE price > 100) AS d
  ORDER BY d.name;
COMMIT;
SELECT * FROM (SELECT name FROM fruit);

-- ============ 集合演算 ============
BEGIN READ ONLY;
SELECT name FROM fruit UNION SELECT name FROM fruit ORDER BY 1;
SELECT price FROM fruit EXCEPT SELECT 100 FROM fruit;
COMMIT;

-- ============ まだ書けない構文(黙って無視せず構文エラーになる) ============
SELECT * FROM fruit WHERE price BETWEEN 1 AND 2;
-- RIGHT/FULL は予約語にしてある。さもないと「right という別名の内部結合」
-- として黙って通ってしまう
SELECT * FROM fruit RIGHT JOIN fruit f2 ON 1 = 1;

-- ============ DDLはトランザクションの中では実行できない ============
BEGIN;
CREATE TABLE t2 (a INTEGER);
ROLLBACK;

-- ============ 後始末 ============
DROP TABLE fruit;
