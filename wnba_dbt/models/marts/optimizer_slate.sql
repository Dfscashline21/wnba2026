with dk as (

    select
        "Position",
        "ID",
        "Salary",
        "Game Info",
        "TeamAbbrev",
        "Name + ID",
        lower(trim(name)) as name_norm
    from {{ ref('clean_draftkings') }}

),

medians_norm as (

    select
        lower(trim(player_name)) as name_norm,
        "TeamAbbrev",
        max(dkpts) as dkpts
    from {{ ref('medians') }}
    group by 1, 2

),

standard_norm as (

    select
        lower(trim(player_name)) as name_norm,
        max(coalesce(cast(dkstd as double precision), 0::double precision)) as dkstd
    from {{ ref('standard') }}
    group by 1

),

joined as (

    select
        dk."Position",
        dk."ID",
        dk."Salary",
        dk."Game Info",
        dk."TeamAbbrev",
        dk."Name + ID",
        m.dkpts,
        s.dkstd,
        m.dkpts + (1.5 * s.dkstd) as ceiling,
        m.dkpts - (1.5 * s.dkstd) as floor
    from dk
    inner join medians_norm m
        on dk.name_norm = m.name_norm
        and dk."TeamAbbrev" = m."TeamAbbrev"
    inner join standard_norm s
        on dk.name_norm = s.name_norm

)

select
    "Position",
    "ID",
    "Salary",
    "Game Info",
    "TeamAbbrev",
    "Name + ID",
    dkpts,
    dkstd,
    ceiling,
    floor
from joined
