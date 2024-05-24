-- Data Cleaning Project - World Layoffs 
-- Jasiel Austin 2024
-- Creating a database, importng a dataset and cleaning the data


select *
from layoffs
;

-- 1. Remove duplicates if any
-- 2. Standardize the data to find and fix any issues
-- 3. Review null or blank values to see if manual population is feasible
-- 4. Remove columns and rows that are not necessary


create table layoffs_staging	-- Try not to work off the raw data table when editing and manipulating 
like layoffs					-- Copy over data fields from this table into the newly created one
;

select *
from layoffs_staging
;

insert layoffs_staging
select*
from layoffs
;

-- 1. Remove Duplicates (without a unique column ID)

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num -- 'Date' column name is back ticked because it is a keywork in SQL
-- If row number is > 1 hen there is a duplicate in the data set
from layoffs_staging;

with duplicate_cte as (   
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1
;

select *
from layoffs_staging
where company = 'Casper'  -- Double checking that the duplicates in the cte are correct for the resulting companies showed
;

-- Creating a new table to delete the duplicates found from the cte run done above. Copied from layoff_staging table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int												-- Added to get the duplicate indicator found in the above cte
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
;

-- Now adding cte code to get the resulting duplicat indicator
-- Now we can filter for row number to find and delete duplicates
insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1
;

delete 
from layoffs_staging2
where row_num > 1
;

select *
from layoffs_staging2
;

-- 2. Standardizing Data 

select company, (trim(company))
from layoffs_staging2
;

update layoffs_staging
set company = trim(company)
;

select distinct industry -- Shows the industry column contents but not for each row, only the unique values
from layoffs_staging2
order by 1 -- The column you'd like to order by (in this one column view it would just be 1 or industry
;
-- Found that crypto is in multiple times but in different ways

select *
from layoffs_staging2
where industry like 'Crypto%' 
;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1
;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%'
;

-- updating date column from text for data visualization 
select `date`,
str_to_date(`date`, '%m/%d/%Y' )  -- items in bracket is the nomenclature for the month day year values in the current data to update it to the standard date format. Capital Y takes the full year values and common y takes the first two year values as the year
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y' )
;

alter table layoffs_staging2
modify column `date` date; 				-- Can be done now that the format has been updated 


-- 3. Null and Blank Values
select *
from layoffs_staging2
where total_laid_off is null		-- When using NULL, 'IS' is used vs 'LIKE' which has been used 
and percentage_laid_off is null
;

update layoffs_staging2
set industry = null
where industry = ''
;

select *
from layoffs_staging2
where industry is null 
or industry = ''
;

select *
from layoffs_staging2
where company like 'Bally%'
;

select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;

update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
    and t1.location = t2.location
    set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null
;

-- 4. Removing Columns and Rows

delete
from layoffs_staging2
where total_laid_off is null	
and percentage_laid_off is null
;

-- Since total laid off and percentage laid off values from the abuv are null and that information will be important to the visualization/ exloratory portion, they potentially can be deleted

select *
from layoffs_staging2
;

alter table layoffs_staging2
drop column row_num 			-- columns are dropped while rows are deletat. Different syntax
;
