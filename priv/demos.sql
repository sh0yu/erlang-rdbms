# デモ台本。web_ui が読んで画面の左に並べる。
#
#   === タイトル          新しいデモが始まる
#   --- 説明              そのデモの説明(複数行可)
#   -- 手順の説明          次の1文の説明
#   @A / @B               その文を流す接続。省略すると A
#   !error                次の1文は失敗するのが正しい
#   SQL;                  ; までが1文
#
# 接続が2本あるのは、並行制御は1本では見せられないため。
# A と B は別のトランザクションとして走る。

=== 1. まず動かす
--- 表を作って入れて読む。SQL がストレージまで届いていることを確かめる。
--- 右の「実行計画」に、その SELECT がどう実行されるかが出る。

-- 表を作る。DDL はトランザクションの外で暗黙に確定する
CREATE TABLE fruit (id INTEGER PRIMARY KEY, name VARCHAR(20), price INTEGER);

-- 書き込みにはトランザクションが要る
BEGIN;

-- 4 行入れる
INSERT INTO fruit VALUES (1, 'apple', 100);
INSERT INTO fruit VALUES (2, 'banana', 150);
INSERT INTO fruit VALUES (3, 'orange', 150);
INSERT INTO fruit VALUES (4, 'grape', 300);

-- コミットするまで、この変更は自分からしか見えない
COMMIT;

-- 全部読む
SELECT * FROM fruit ORDER BY id;

-- 条件をつける。計画に Filter が入る
SELECT name, price FROM fruit WHERE price >= 150 ORDER BY price, name;

=== 2. プランナ(索引を張ると計画が変わる)
--- 同じ SELECT でも、索引と統計があるかどうかで実行方法が変わる。
--- 右の「実行計画」を見ながら進めると、Seq Scan が Index Scan に
--- 置き換わるところが見える。

-- 索引が無いので全表走査になる
SELECT * FROM fruit WHERE name = 'banana';

-- 索引を作る
CREATE INDEX fruit_name ON fruit (name);

-- 同じ SELECT。索引スキャンに変わる
SELECT * FROM fruit WHERE name = 'banana';

-- 統計を採る。行数と異なり値の数を記録する
ANALYZE fruit;

-- 選択率が低ければ索引、高ければ全表走査。ここは全部に当たるので走査
SELECT * FROM fruit WHERE price > 0;

-- 計画だけを見る。EXPLAIN は実行しない
EXPLAIN SELECT * FROM fruit WHERE name = 'apple';

=== 3. 結合(入れ子ループとハッシュ)
--- 等値で結べるならハッシュ結合、そうでなければ入れ子ループ。
--- プランナがどちらを選んだかは計画に出る。

-- 産地の表を足す
CREATE TABLE origin (fruit_id INTEGER, country VARCHAR(20));

BEGIN;
INSERT INTO origin VALUES (1, 'Japan');
INSERT INTO origin VALUES (2, 'Philippines');
INSERT INTO origin VALUES (3, 'USA');
COMMIT;

-- 等値結合。Hash Join が選ばれる
SELECT f.name, o.country FROM fruit f JOIN origin o ON f.id = o.fruit_id ORDER BY f.name;

-- 表名を書かなくても、どの表の列かはカタログから分かる
SELECT name, country FROM fruit, origin WHERE id = fruit_id ORDER BY name;

-- 等値でない条件は入れ子ループになる
SELECT f.name, o.country FROM fruit f JOIN origin o ON f.id > o.fruit_id ORDER BY f.name;

-- 左外部結合。産地の無い grape も残る
SELECT f.name, o.country FROM fruit f LEFT JOIN origin o ON f.id = o.fruit_id ORDER BY f.name;

=== 4. NULL は「不明」
--- NULL は値ではなく「分からない」。比較すると真でも偽でもない
--- (unknown)になり、WHERE は真の行しか通さない。

BEGIN;
INSERT INTO origin VALUES (4, NULL);
COMMIT;

-- NULL は = でも <> でも引っかからない
SELECT * FROM origin WHERE country = 'Japan';

-- 「Japan でない」でも NULL の行は出てこない
SELECT * FROM origin WHERE country <> 'Japan';

-- NULL を取るには IS NULL
SELECT * FROM origin WHERE country IS NULL;

-- 集約は NULL を入力から外す。COUNT(*) だけが例外
SELECT COUNT(*), COUNT(country) FROM origin;

-- CASE も同じ。条件が unknown の枝は選ばれない
SELECT fruit_id, CASE WHEN country = 'Japan' THEN 'domestic' ELSE 'other' END FROM origin ORDER BY fruit_id;

=== 5. トランザクション(捨てられる)
--- コミットしていない変更は共有データに書かれていない。
--- ロールバックは、書く前の記録を捨てるだけで済む。

BEGIN;

-- 値を変える
UPDATE fruit SET price = 999 WHERE name = 'apple';

-- 自分からは見える
SELECT name, price FROM fruit WHERE name = 'apple';

-- 捨てる
ROLLBACK;

-- 元のまま
SELECT name, price FROM fruit WHERE name = 'apple';

=== 6. 列制約
--- PRIMARY KEY は UNIQUE かつ NOT NULL。
--- 一意性の検査だけは分離水準の外にあり、共有データを直に引く。

BEGIN;

-- 主キーが重複するので断られる
!error
INSERT INTO fruit VALUES (1, 'apple2', 120);

-- 主キーは NULL にできない
!error
INSERT INTO fruit VALUES (NULL, 'nothing', 1);

ROLLBACK;

=== 7. スナップショット(読み手は書き手を待たない)
--- A と B は別のトランザクション。B がコミットしても、
--- 先に始まった A の見え方は変わらない。
--- 右の「エンジンの状態」で、A と B が違う版を見ているのが分かる。

-- A が読み取り専用で始める。ここで版を1つ押さえる
@A BEGIN READ ONLY;

-- A から見た値
@A SELECT name, price FROM fruit WHERE name = 'banana';

-- B が別のトランザクションで値を変えてコミットする
@B BEGIN;
@B UPDATE fruit SET price = 777 WHERE name = 'banana';
@B COMMIT;

-- B は待たされていない。A もまだ古い値を見ている
@A SELECT name, price FROM fruit WHERE name = 'banana';

-- A を閉じる
@A COMMIT;

-- 開き直せば新しい値
@A BEGIN READ ONLY;
@A SELECT name, price FROM fruit WHERE name = 'banana';
@A COMMIT;

=== 8. 分離水準(同じ行を2本で書く)
--- 既定(REPEATABLE READ)は、自分が読んだ後に他人が同じ行を変えていたら
--- 断る。断らないと、先にコミットした方の更新が消える。

@A BEGIN;
@B BEGIN;

-- 双方が同じ値を見る
@A SELECT name, price FROM fruit WHERE name = 'grape';
@B SELECT name, price FROM fruit WHERE name = 'grape';

-- A が先に書いてコミットする
@A UPDATE fruit SET price = 310 WHERE name = 'grape';
@A COMMIT;

-- B は古い値を見たまま書こうとする。UPDATE の時点で断られる
!error
@B UPDATE fruit SET price = 320 WHERE name = 'grape';

-- 断られた側はロールバック済み。閉じるものがもう無い
!error
@B COMMIT;

-- A の値が残っている
@A BEGIN READ ONLY;
@A SELECT name, price FROM fruit WHERE name = 'grape';
@A COMMIT;

=== 9. READ COMMITTED(断らずに読み直す)
--- 文ごとに版を取り直す。ロックを待たされた後は、待っている間に入った
--- 変更を読み直して文をやり直すので、断られない。
--- 代わりに、同じトランザクションの中で2度読むと違う結果が返りうる。

@A BEGIN;
@B BEGIN READ COMMITTED;

-- B が読む
@B SELECT name, price FROM fruit WHERE name = 'orange';

-- A が変えてコミットする
@A UPDATE fruit SET price = 160 WHERE name = 'orange';
@A COMMIT;

-- B からは値が変わって見える(反復可能読み取りを失っている)
@B SELECT name, price FROM fruit WHERE name = 'orange';

-- 断られずに書ける。読み直した後の値に足される
@B UPDATE fruit SET price = price + 5 WHERE name = 'orange';
@B COMMIT;

-- 165 になっている。160 を踏み潰していない
@A BEGIN READ ONLY;
@A SELECT name, price FROM fruit WHERE name = 'orange';
@A COMMIT;

=== 10. 行ロック(同じ行は待つ)
--- 書き込みロックは行ごと。別の行なら待たない。
--- ここでは B の UPDATE が A のコミットまで返ってこない。

@A BEGIN;
@A UPDATE fruit SET price = 101 WHERE name = 'apple';

-- 別の行なので B は待たない
@B BEGIN;
@B UPDATE fruit SET price = 301 WHERE name = 'grape';
@B COMMIT;

-- A を閉じる
@A COMMIT;

=== 11. 集約・副問い合わせ・集合演算
--- 一通りの SQL。計画がどう組まれるかを右で見る。

-- 集約とグループ化
SELECT price, COUNT(*) FROM fruit GROUP BY price HAVING COUNT(*) > 1 ORDER BY price;

-- 相関しない副問い合わせ。実行前に一度だけ畳まれる
SELECT name FROM fruit WHERE price > (SELECT AVG(price) FROM fruit) ORDER BY name;

-- 相関する副問い合わせ。外側の行ごとに実行し直す
SELECT name FROM fruit f WHERE EXISTS (SELECT 1 FROM origin o WHERE o.fruit_id = f.id) ORDER BY name;

-- 集合演算
SELECT name FROM fruit WHERE price >= 150 UNION SELECT name FROM fruit WHERE id = 1 ORDER BY 1;

-- 導出表
SELECT t.name FROM (SELECT name, price FROM fruit WHERE price < 200) AS t ORDER BY t.name;

-- BETWEEN と CASE
SELECT name, CASE price WHEN 100 THEN 'cheap' ELSE 'other' END FROM fruit WHERE price BETWEEN 100 AND 200 ORDER BY name;
