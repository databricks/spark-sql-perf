-- using 1499680242 as a seed to the RNG


select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone, 1, 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone, 1, 2) in
				('40', '50', '60', '70', '80', '12', '18')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone, 1, 2) in
						('40', '50', '60', '70', '80', '12', '18')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
