with clean_columns as (

select 
    "Datum" as datum, 
    "Tijd" as tijd,
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
    datum,
    tijd,
    product,
    isin,
    beurs,
    uitvoeringsplaats,
    aantal,
    koers,
    valuta,
    abs(lokale_waarde) as lokale_waarde,
    abs(waarde) as waarde,
    wisselkoers
    transactiekosten,
    abs(totale_bedrag) as totale_bedrag,
    order_id,
    case when waarde < 0 then 'aankoop' else 'verkoop' end as transactietype
from 
        clean_columns