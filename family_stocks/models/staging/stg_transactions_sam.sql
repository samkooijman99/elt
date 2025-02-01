with clean_columns as (

select 
    "Datum" as datum, 
    "Product" as product,
    "ISIN" as isin,
    "Beurs" as beurs,
    "Uitvoeringsplaats" as uitvoeringsplaats,
    "Aantal" as aantal,
    "Koers" as koers,
    i as valuta,
    "Lokale waarde" as lokale_waarde, 
    "Waarde" as waarde,
    "Wisselkoers" as wisselkoers,
    "Transactiekosten en/of" as transactiekosten,
    "Totaal" as totale_bedrag,
    "Order ID" as order_id
from 
    {{ ref('transactions_sam') }}

)

select 
    to_date(datum, 'dd-MM-yyyy') as datum,
    product,
    isin,
    beurs,
    uitvoeringsplaats,
    aantal,
    koers,
    valuta,
    cast(abs(lokale_waarde) as float) as lokale_waarde,
    cast(abs(waarde) as float) as waarde,
    wisselkoers,
    cast(abs(transactiekosten) as float) as transactiekosten,
    cast(abs(totale_bedrag) as float) as totale_bedrag,
    order_id,
    case when waarde < 0 then 'aankoop' else 'verkoop' end as transactietype
from 
    clean_columns