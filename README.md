Usage:

```php
[
    'npApiKey' => 'API Key from NP',
    'elasticHost' => 'elastic host for save NP data',
    'localTariffCityName' => 'for local tariff city of warehouse',
    'localTariffCityArea' => 'for local tariff area which check for tariff'
];
```
``` php 
    php artisan np:save //save Ukrainian city's and divisions
    php artisan npi:save //save International countries, city's and divisions
    php artisan np:local-tariff-save //parse local tariff data
```

``` php 
    UIArts\NPDivisions\Services\NPService //service for Ukrainian NP
    UIArts\NPDivisions\Services\NPIService //service for International NP
```