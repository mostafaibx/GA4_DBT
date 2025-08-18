{{ 
  config(
    materialized = 'table',
    tags = ['ga4','dim'],
  ) 
}} 


with spine as (
  -- Build a continuous date array from a fixed start to (today + future_days)
  select d as date_day
  from unnest(
    generate_date_array(
      date('{{ var("date_spine_start") }}'),
      date_add(current_date(), interval {{ var("date_spine_future_days") }} day),
      interval 1 day
    )
  ) as d
),
base as (
  select
    -- Keys
    {{ make_date_key('date_day') }}                        as date_key,
    date_day                                               as date,

    -- Simple parts
    extract(year  from date_day)                           as year,
    extract(quarter from date_day)                         as quarter,
    extract(month from date_day)                           as month,
    extract(day   from date_day)                           as day_of_month,
    extract(dayofyear   from date_day)                     as day_of_year,

    -- Names
    format_date('%A', date_day)                            as day_name,
    format_date('%B', date_day)                            as month_name,
    format_date('%Y-%m', date_day)                         as yyyymm,
    format_date('%F', date_day)                            as iso_date,           -- YYYY-MM-DD

    -- ISO week fields (Mon–Sun)
    extract(isoweek from date_day)                         as iso_week,
    extract(isoyear from date_day)                         as iso_year,
    date_trunc(date_day, isoweek)                          as iso_week_start,
    date_add(date_trunc(date_day, isoweek), interval 6 day) as iso_week_end,
    extract(dayofweek from date_day)                    as iso_day_of_week,    -- 1=Mon..7=Sun
    case when extract(dayofweek from date_day) in (6,7) then true else false end as is_weekend,

    -- Month / quarter / year spans
    date_trunc(date_day, month)                            as month_start,
    date_sub(date_add(date_trunc(date_day, month), interval 1 month), interval 1 day) as month_end,
    date_trunc(date_day, quarter)                          as quarter_start,
    date_sub(date_add(date_trunc(date_day, quarter), interval 1 quarter), interval 1 day) as quarter_end,
    date_trunc(date_day, year)                             as year_start,
    date_sub(date_add(date_trunc(date_day, year), interval 1 year), interval 1 day) as year_end,

    -- Period flags
    case when date_day = date_trunc(date_day, month) then true else false end     as is_month_start,
    case when date_day = date_sub(date_add(date_trunc(date_day, month), interval 1 month), interval 1 day) then true else false end as is_month_end,
    case when date_day = date_trunc(date_day, quarter) then true else false end   as is_quarter_start,
    case when date_day = date_sub(date_add(date_trunc(date_day, quarter), interval 1 quarter), interval 1 day) then true else false end as is_quarter_end,
    case when date_day = date_trunc(date_day, year) then true else false end      as is_year_start,
    case when date_day = date_sub(date_add(date_trunc(date_day, year), interval 1 year), interval 1 day) then true else false end as is_year_end,

    -- Fiscal calendar (FY named by ending year; e.g., Apr-2025→Mar-2026 is FY 2026 if fiscal_year_start_month=4)
    {{ var('fiscal_year_start_month') }}                                                          as fiscal_year_start_month,
    case 
      when {{ var('fiscal_year_start_month') }} = 1 then extract(year from date_day)
      when extract(month from date_day) >= {{ var('fiscal_year_start_month') }} then extract(year from date_day) + 1
      else extract(year from date_day)
    end                                                       as fiscal_year,
    (MOD(extract(month from date_day) - {{ var('fiscal_year_start_month') }} + 12, 12) + 1)     as fiscal_month_number,
    (DIV(MOD(extract(month from date_day) - {{ var('fiscal_year_start_month') }} + 12, 12), 3) + 1) as fiscal_quarter,
    -- Fiscal period starts/ends
    date_add(date_trunc(date_sub(date_add(date_day, interval (13-{{ var('fiscal_year_start_month') }}) month), interval 12 month), year),
             interval ({{ var('fiscal_year_start_month') }}-1) month)                           as fiscal_year_start,
    date_sub(date_add(
              date_add(date_trunc(date_sub(date_add(date_day, interval (13-{{ var('fiscal_year_start_month') }}) month), interval 12 month), year),
                       interval ({{ var('fiscal_year_start_month') }}-1) month),
              interval 1 year),
            interval 1 day)                                                     as fiscal_year_end

  from spine
)

select * from base
order by date;
