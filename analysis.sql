/********************************
Load data into relational database table
********************************/
drop table if exists jeffli.posts;
create table jeffli.posts
(
blog_id varchar(48)
, post_id varchar(48)
, lang varchar(48)
, url varchar(254)
, date_gmt timestamp
, title varchar(254)
, content varchar(1024)
, author varchar(254)
, author_login varchar(254)
, author_id varchar(48)
, liker_ids varchar(10000)
, like_count int
, commenter_ids varchar(4000)
, comment_count int
)
;


copy jeffli.posts from 's3://<REDACTED>/posts.jsonl'
CREDENTIALS 'aws_access_key_id=<REDACTED>;aws_secret_access_key=<REDACTED>'
json 's3://<REDACTED>/jsonpath.json'
TRUNCATECOLUMNS
maxerror as 10000
;


-- 1,809,199 rows loaded with no errors



/********************************
Exploration & Understanding the data structure
********************************/


-- How many rows are in the sample of data?
-- 1,809,199 rows
select count(*)
from jeffli.posts
where liker_ids is not null
;



-- what do the data look like?
select *
from jeffli.posts
order by post_id
limit 1000
;



-- what is the granularity of the data? what is a good primary key for the table?
-- the primary key is blog_id + post_id,
-- where each row corresponds to an individual post from a blog
select like_count
, count(*) n_rows
, count(distinct blog_id) as blogs
, count(distinct blog_id || post_id) as posts
from jeffli.posts
group by 1
order by 1
;


/********************************
Assignment Questions
********************************/


/********************************
*
* Question #1:
* What are the median and the mean numbers of likes per post in this data sample?
*
********************************/

-- Answer
-- average likes per post is 4.2387
-- median likes per post is 0
select avg(like_count::float) as mean_posts
, median(like_count) as median_posts
from jeffli.posts
;

-- understand distribution of likes per post
-- it has a long right tail
-- bottom 52% of posts have 0 likes.  50% of likes were from 5% of posts.
select *
, sum(posts) over(order by n_likes rows unbounded preceding) as cumulative_posts
, sum(likes_created) over(order by n_likes rows unbounded preceding) as cumulative_likes_created
, cumulative_posts::float / sum(posts) over() as cum_pct_of_posts
, cumulative_likes_created::float / sum(likes_created) over() as cum_pct_of_likes_created
from
(
    select like_count as n_likes
    , count(*) as posts
    , n_likes * posts as likes_created
    from jeffli.posts
    group by 1
)
order by 1
;


/********************************
*
* Question #2
* What is the mean number of posts per author in this data sample?
*
********************************/

-- Answer:
-- mean posts per poster = 122.7
-- median posts per poster = 24
select avg(posts::float) as mean_posts_per_poster
, median(posts) as median_posts
from
(
select author_id
, count(*) as posts
from jeffli.posts
group by 1
order by 1
)
;

-- understand distribution of posts per author
-- it has a long right tail
-- top 3-4% of posters made 50% of posts
select *
, sum(posters) over(order by n_posts rows unbounded preceding) as cumulative_posters
, sum(posts_created) over(order by n_posts rows unbounded preceding) as cumulative_posts_created
, cumulative_posters::float / sum(posters) over() as cum_pct_of_posters
, cumulative_posts_created::float / sum(posts_created) over() as cum_pct_of_posts_created
from
(
    select n_posts
    , count(distinct author_id) as posters
    , posters*n_posts as posts_created

    from
    (
        select author_id
        , count(*) as n_posts
        from jeffli.posts
        group by 1
    )
    group by 1
)
order by 1
;

/********************************
*
* Question #3
* How many of the authors in this sample have not liked any of the posts in this sample?
*
********************************/

-- First, Understand Data:
-- Can a user only leave one like per post at a time?
-- i.e. validate that the like_count corresponds to the number of liker_ids on a post.
select count(*) n_rows
, count(case when like_count = regexp_count(liker_ids, ',') + 1 then like_count end) n_rows_with_matching_counts
, n_rows_with_matching_counts::float / n_rows as pct_rows_matching
from jeffli.posts
where (like_count > 0 or liker_ids <> '')
;

-- n_rows	n_rows_with_matching_counts	pct_rows_matching
-- 856214	856214	1


-- This is true -- 100% of rows have these counts matching.
-- So users can leave max 1 like on a post.  A post's total like count is equal to the count of unique users who've liked it.

-- This means I can create a table containing every like
-- where each row corresponds to a like on a post, and contains a unique combination of blog_id, post_id, liker_id (== author id)



-- So, let's create this "post_likes" table

-- check maximum number of likes on a post
-- 1063 likes
select max(like_count)
from jeffli.posts
;

-- helper table for splitting liker_ids (array) string on a delimiter
drop table if exists #seq;
create table #seq diststyle all sortkey(i) as
select row_number() over() as i
from jeffli.posts
limit 1063 -- max number of likes on a post
;

drop table if exists jeffli.post_likes;
create table jeffli.post_likes distkey(liker_id) as
select blog_id
, post_id
, split_part(replace(replace(liker_ids,'[',''),']',''), ',', seq.i::int) as liker_id
from jeffli.posts
cross join #seq seq
where liker_ids is not null
	and i <= like_count
;

-- validate that likes are consistent between liker_ids in post_likes table and sum of like_count in posts table.
-- 7,668,715 total likes
select count(*)
from jeffli.post_likes
;

-- 7,668,715 total likes -- checks out!
select sum(like_count)
from jeffli.posts
;


-- Finally, back to our question.
-- Using our posts and post_likes tables,
-- let's count how many posters never liked any post

select count(distinct a.author_id) posters
, count(distinct b.liker_id) posters_liked_a_post
, count(distinct case when b.liker_id is null then a.author_id end) posters_never_liked_a_post
from jeffli.posts a
left join jeffli.post_likes b
	on a.author_id = b.liker_id
;

-- posters	posters_liked_a_post	posters_never_liked_a_post
-- 14743	8341	6402

-- Answer:
-- 6,402 posters (43%) of total posters in the sample never liked any post.
