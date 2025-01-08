<?php

return [
    'npApiKey' => env('NOVA_POSHTA_API_KEY'),
    'elasticHost' => explode(',', env('ELASTICSEARCH_HOSTS')),
    'localTariffCityName' => 'Київ',
    'localTariffCityArea' => 'Київська',
];
