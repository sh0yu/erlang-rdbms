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

-- ============ まだ書けない構文(黙って無視せず構文エラーになる) ============
BEGIN;
SELECT * FROM fruit WHERE price = (SELECT price FROM fruit);
COMMIT;

-- ============ DDLはトランザクションの中では実行できない ============
BEGIN;
CREATE TABLE t2 (a INTEGER);
ROLLBACK;

-- ============ 後始末 ============
DROP TABLE fruit;
