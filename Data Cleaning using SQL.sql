


show tables;
select * from layoffs_new;

-- 1.Remove Duplicates
-- 2.Standardise the data
-- 3.Null values or blank values
-- 4.Remove any column 


-- staging the dataset 

create table layoffs_staging 
like layoffs_new;

insert layoffs_staging
select * from layoffs_new;

select * from layoffs_staging;

select * ,
row_number() over (
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select * ,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete from duplicate_cte where row_num>1
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2 
select * ,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging2;


select * from layoffs_staging2 where row_num >1;

delete from layoffs_staging2 where row_num >1;

-- Data standardization 

select company , trim(company) 
from layoffs_staging2;


update layoffs_staging2 
set company =trim(company);

select distinct company from layoffs_staging2;

select distinct industry from layoffs_staging2 order by 1; -- we see crypto has a problem here 


select * from 
layoffs_staging2 where industry like 'Crypto%';

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%'; 


select distinct country from layoffs_staging2 order by 1; -- we see usa has a problem here 

update layoffs_staging2 
set country = 'United States'
where country like 'United States%'; 

select `date` from layoffs_staging2;


select * from layoffs_staging2;

select `date`, 
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y')
 ;

UPDATE layoffs_staging2
SET `date` =
case  
WHEN `date` IS NOT NULL AND `date` != 'NULL' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
        ELSE NULL  
    END;

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;


select `date` from layoffs_staging2;

-- looking for null values 

select * from layoffs_staging2
where 
percentage_laid_off is null;


select * from layoffs_staging2;

select * from layoffs_staging2 where 
total_laid_off is NULL;

SELECT total_laid_off
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL AND total_laid_off NOT REGEXP '^-?[0-9]+$';

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off IS NOT NULL AND total_laid_off NOT REGEXP '^-?[0-9]+$';

UPDATE layoffs_staging2
SET total_laid_off = CAST(total_laid_off AS UNSIGNED)
WHERE total_laid_off IS NOT NULL;

ALTER TABLE layoffs_staging2 MODIFY COLUMN total_laid_off INT;


UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off IS NOT NULL AND percentage_laid_off NOT REGEXP '^-?[0-9]+(\.[0-9]+)?$';


-- 
select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;



select distinct industry from layoffs_staging2;

select * from layoffs_staging2 where
industry = 'NULL' or 
industry ='' ; -- few null values in industry 


Alter table layoffs_staging2 drop column row_num;

select count(*) from layoffs_staging2;










