USE [app_customers]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO
ALTER procedure [comdirect].[s_export_kag] @f_lieferung_id int as

/*
_______________________________________________________________________________

 Liefert die Daten für die KAG-Lieferung an die Comdirect

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
	id varchar(20) null,
	name varchar(255) null
)

/* Erste Zeile eintragen */

set @datum = getdate();

set @datumExport = convert(varchar(10), @datum, 112)

insert #export
	(SKZ, id, name)
values
	('S', @datumExport, null)


/* Datenzeilen eintragen */


insert #export
	(SKZ, id, name)
select 'D' as SKZ, id, name
from fonds_fww.dbo.femittent
where id in
(
select c.f_femittent_id
from fonds_fww.dbo.ffee as f
left join (select id, isin, f_femittent_id from fonds_fww.dbo.fcore) as c on c.id = f.f_fcore_id
left join fonds_fww.dbo.fwknland as wkn on wkn.f_fcore_id = f.f_fcore_id and wkn.f_fland_id = 1
where f.f_fcore_id in 
(
select f_fcore_id 
from fonds_fww.dbo.flizenzenfondskey
where f_flizenzen_id = @f_flizenzen_id
)
group by c.f_femittent_id
)
order by id asc


/* Letzte Zeile eintragen */

set @rowcount = (select count(*) - 1 from #export)

insert #export
	(SKZ, id, name)
values
	('E', @rowcount, null)


/* Daten für die Datei ausgeben */

select SKZ, id, name
from #export
order by rowid asc



/* Clean up */

drop table #export