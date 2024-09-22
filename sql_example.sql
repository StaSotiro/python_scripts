with countryRatings as (
	select country, avg(rating) as avgRating
	from tourism_dataset td 
	group by country
),
countryCatAvgs as (
	select country, category , avg(rating) as catAvgRating
	from tourism_dataset td2 
	group by country, category 
),
countryPartitioned as (
	select country, category, catAvgRating,
	row_number() over (partition by country order by catAvgRating desc) as orderRank
	from countryCatAvgs
)
select cr.country, cr.avgRating, cp.category, cp.catAvgRating
from countryRatings cr 
join countryPartitioned cp on cr.country = cp.country 
where cp.orderRank <= 3
order by cr.country asc, cp.catAvgRating desc
;