USE [app_customers]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO
ALTER procedure [comdirect].[s_export_fees] @f_lieferung_id int as

/*
_______________________________________________________________________________

 Liefert die Daten für die Gebühren-Lieferung an die Comdirect

_______________________________________________________________________________

[v1.0] - [28.11.2019] - kh@fww.de

*/

declare @f_flizenzen_id int;
set @f_flizenzen_id = 67

declare @datum datetime;
declare @datumExport varchar(10); 
declare @rowcount int;

create table #export
(
	rowid int identity(1,1) not null,
	SKZ varchar(3) null,
	WKN varchar(20) null,
	ISIN varchar(20) null,
	KAG varchar(255) null,
	REGAA varchar(30) null,
	METHODE varchar(2) null,
	AAAP varchar(30) null,
	AANAV varchar(30) null
)

/* Erste Zeile eintragen */

set @datum = getdate();

set @datumExport = convert(varchar(10), @datum, 112)

insert #export
	(SKZ, WKN, ISIN, KAG, REGAA, METHODE, AAAP, AANAV)
values
	('S', @datumExport, null, null, null, null, null, null)


/* Datenzeilen eintragen */


insert #export
	(SKZ, WKN, ISIN, KAG, REGAA, METHODE, AAAP, AANAV)

select 
'D' as 'SKZ',
wkn.wkn_alpha as 'WKN',
c.isin as 'ISIN',
c.f_femittent_id as 'KAG',

case 
	when f.f_fafeequelle_id = 1 and e.f_fcore_id is null then replace(convert(varchar(40), f.afee), '.', ',')
	when f.f_fafeequelle_id = 1 and e.f_fcore_id is not null then replace(convert(varchar(40), e.afee), '.', ',')
	when f.f_fafeequelle_id = 2 and e.f_fcore_id is null then replace(convert(varchar(40), f.afeebrutto), '.', ',')
	when f.f_fafeequelle_id = 2 and e.f_fcore_id is not  null then replace(convert(varchar(40), e.afeebrutto), '.', ',')
	when f.f_fafeequelle_id = 3 and e.f_fcore_id is null then replace(convert(varchar(40), f.afee), '.', ',')
	when f.f_fafeequelle_id = 3 and e.f_fcore_id is not  null then replace(convert(varchar(40), e.afee), '.', ',')
	else null
end
as 'REGAA',
case f.f_fafeequelle_id
	when 1 then 'N'
	when 2 then 'B'
	when 3 then 'N'
	else 'U'
end
as 'METHODE',
case
	when e.f_fcore_id is null then  replace(convert(varchar(40), f.afeebrutto), '.', ',')
	else replace(convert(varchar(40), e.afeebrutto), '.', ',')
	end
as 'AAAP',
case
	when e.f_fcore_id is null then  replace(convert(varchar(40), f.afee), '.', ',')
	else replace(convert(varchar(40), e.afee), '.', ',')
	end
as 'AANAV'
from fonds_fww.dbo.ffee as f
left join (select id, isin, f_femittent_id from fonds_fww.dbo.fcore) as c on c.id = f.f_fcore_id
left join fonds_fww.dbo.fwknland as wkn on wkn.f_fcore_id = f.f_fcore_id and wkn.f_fland_id = 1
left join konditionen.dbo.fkoncomdirect_fees as e on e.f_fcore_id = f.f_fcore_id
where f.f_fcore_id in 
(
select f_fcore_id 
from fonds_fww.dbo.flizenzenfondskey
where f_flizenzen_id = @f_flizenzen_id
)



/* Letzte Zeile eintragen */

set @rowcount = (select count(*) - 1 from #export)

insert #export
	(SKZ, WKN, ISIN, KAG, REGAA, METHODE, AAAP, AANAV)
values
	('E', @rowcount, null, null, null, null, null, null)




/* Daten für die Datei ausgeben */

select SKZ, WKN, ISIN, KAG, REGAA, METHODE, AAAP, AANAV
from #export
order by rowid asc



/* Clean up */

drop table #export