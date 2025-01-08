<?php

namespace UIArts\NPDelivery\Services;

use Elasticsearch\ClientBuilder;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Cache;

class NPService
{
    public const CITIES_INDEX = 'np_cities';
    public const DIVISIONS_INDEX = 'np_divisions';
    public const POSTOMAT_TYPE = 'postomat';
    public const DIVISION_TYPE = 'division';

    private $esClient;

    public function __construct()
    {
        $this->esClient = ClientBuilder::create()->setHosts(config('np-delivery.elasticHost'))->build();
    }

    public function getCities()
    {
        $params = [
            'index' => self::CITIES_INDEX,
            'body' => [
                'size'   => 100000,
                'sort' => [
                    [
                        '_script' => [
                            'type' => 'number',
                            'script' => [
                                'source' => "return doc['ref'].value == '8d5a980d-391c-11dd-90d9-001a92567626' ? 0 : 1",
                                'lang' => 'painless'
                            ],
                            'order' => 'asc'
                        ]
                    ],
                    [
                        '_script' => [
                            'type' => 'number',
                            'script' => [
                                'source' => "return Integer.parseInt(doc['cityID'].value);",
                                'lang' => 'painless',
                            ],
                            'order' => 'asc',
                        ],
                    ],
                ],
                '_source' => ['ref', 'localTariff'],
                'script_fields' => [
                    'description' => [
                        'script' => [
                            'source' => "return params.locale == 'ru' ? doc['descriptionRu'].value : doc['description'].value",
                            'params' => ['locale' => App::getLocale()]
                        ]
                    ],
                    'typeCity' => [
                        'script' => [
                            'source' => "return (params.locale == 'ru') ? doc['typeCityRu'].value : doc['typeCity'].value",
                            'params' => ['locale' => App::getLocale()]
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(fn ($hit) => [
                'description' => $hit['fields']['description'][0] ?? null,
                'typeCity' => $hit['fields']['typeCity'][0] ?? null,
                'ref' => $hit['_source']['ref'] ?? null,
                'localTariff' => $hit['_source']['localTariff'] ?? null,
            ], $response['hits']['hits']);
        }

        return null;
    }

    public function getWarehouse($cityRef, $type = null)
    {
        $query = ['term' => ['cityRef.value' => $cityRef]];
        if ($type) {
            $query = [
                'bool' => [
                    'must' => [
                        ['term' => ['cityRef' => $cityRef]],
                        ['term' => ['typeOfWarehouse' => $type]],
                    ],
                ]
            ];
        }

        $params = [
            'index' => self::DIVISIONS_INDEX,
            'body' => [
                'size' => 100000,
                'sort' => [
                    [
                        '_script' => [
                            'type' => 'number',
                            'script' => [
                                'source' => "
                                    String description = doc['description'].value;
                                    Matcher matcher = /№\\s*(\\d+)/.matcher(description);
                                    return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
                                ",
                                'lang' => 'painless',
                            ],
                            'order' => 'asc',
                        ],
                    ]
                ],
                'query' => $query,
                '_source' => ['ref', 'typeOfWarehouse', 'cityRef', 'siteKey'],
                'script_fields' => [
                    'description' => [
                        'script' => [
                            'source' => "return params.locale == 'ru' ? doc['descriptionRu'].value : doc['description'].value",
                            'params' => ['locale' => App::getLocale()]
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(fn ($hit) => [
                'siteKey' => $hit['_source']['siteKey'] ?? null,
                'description' => $hit['fields']['description'][0] ?? null,
                'cityRef' => $hit['_source']['cityRef'] ?? null,
                'typeOfWarehouse' => $hit['_source']['typeOfWarehouse'] ?? null,
                'ref' => $hit['_source']['ref'] ?? null,
            ], $response['hits']['hits']);
        }

        return null;
    }

    /**
     * The function of receiving streets city from Cache, file, or API.
     * @param $cityRef
     * @param $search
     * @return bool|mixed|string
     */
    public function getStreets($cityRef, $search = null)
    {
        if (Cache::has('np.streets.'. $cityRef) && $search === null) {
            return Cache::get('np.streets.'. $cityRef);
        }
        $json = $this->streetsApi($cityRef, $search);
        if ($json && $search === null) {
            Cache::put('np.streets.'. $cityRef, $json, 86400);
        }
        return $json;
    }

    /**
     * The function of receiving list streets format from the API of NovaPoshta.
     * @param $cityRef
     * @param $search
     * @return array
     */
    private function streetsApi($cityRef, $search)
    {
        $data = '{
        "apiKey": "' . config('np-delivery.npApiKey') . '",
        "modelName": "Address",
        "calledMethod": "getStreet",
        "methodProperties": {
            "CityRef" : "' . $cityRef . '",
            "FindByString" : "' . $search . '",
            "Page" : "1",
            "Limit" : "10000"
            }
        }';

        $response = $this->send($data);

        $streets = [];
        if ($response) {
            $npStreets = json_decode($response, true)["data"];
            foreach($npStreets as $street) {
                $street = (array) $street;
                $streets[] = [
                    'description' => $street['Description'],
                    'type' => $street['StreetsType'],
                    'typeRef' => $street['StreetsTypeRef'],
                    'ref' => $street['Ref']
                ];
            }
        }
        return $streets;
    }

    /**
     * Function to get CityRef by city name.
     * @param $cityName
     * @return mixed|false
     */
    public function getCityRef($cityName)
    {
        $params = [
            'index' => self::CITIES_INDEX,
            'body' => [
                'query' => [
                    'bool' => [
                        'should' => [
                            ['term' => ['description' => $cityName]],
                            ['term' => ['descriptionRu' => $cityName]]
                        ]
                    ]
                ],
                '_source' => ['ref'],
                'size' => 1
            ]
        ];

        $response = $this->esClient->search($params);

        if (!empty($response['hits']['hits'])) {
            return $response['hits']['hits'][0]['_source']['ref'];
        }

        return null;
    }

    /**
     * Function to get TypeCity by city name.
     * @param string $cityName
     * @param string $language
     * @return mixed|null
     */
    public function getTypeCity(string $cityName, string $language = 'ru')
    {
        $params = [
            'index' => self::CITIES_INDEX,
            'body' => [
                'query' => [
                    'bool' => [
                        'should' => [
                            ['term' => ['description' => $cityName]],
                            ['term' => ['descriptionRu' => $cityName]]
                        ]
                    ]
                ],
                '_source' => ['typeCity', 'typeCityRu'],
                'size' => 1
            ]
        ];

        $response = $this->esClient->search($params);

        if (!empty($response['hits']['hits'])) {
            return $language === 'ru' ? $response['hits']['hits'][0]['_source']['typeCityRu'] : $response['hits']['hits'][0]['_source']['typeCity'];
        }

        return null;
    }

    /**
     * @param string $cityRef
     * @return bool
     */
    public function checkCityForTariffLocal(string $cityRef): bool
    {
        $params = [
            'index' => self::CITIES_INDEX,
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => [
                            ['term' => ['cityRef' => $cityRef]],
                        ],
                    ]
                ],
                '_source' => ['localTariff'],
                'size' => 1
            ]
        ];

        $response = $this->esClient->search($params);

        if (!empty($response['hits']['hits'])) {
            return $response['hits']['hits'][0]['_source']['localTariff'];
        }

        return false;
    }

    /**
     * The function to get tracking orders status from the Nova Poshta API.
     * @param array $data
     * @return array
     */
    public function trackingOrdersStatus(array $data): array
    {
        return json_decode($this->trackingStatusApi(json_encode($data)), true);
    }

    /**
     * Check orders status.
     * @param $json
     * @return bool|string
     */
    private function trackingStatusApi($json)
    {
        $data = '{
        "apiKey": "' . config('np-delivery.npApiKey') . '",
        "modelName": "TrackingDocument",
        "calledMethod": "getStatusDocuments",
        "methodProperties": ' . $json . '
        }';
        return $this->send($data);
    }

    /**
     * The function of sending a request to the NovaPoshta server.
     * @param $json
     * @return bool|string
     */
    private function send($json)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://api.novaposhta.ua/v2.0/json/');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, Array("Content-Type: text/plain"));
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $json);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 30);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 30);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
        $response = curl_exec($ch);
        curl_close($ch);
        if(isset($response['success']) && !$response['success']){
            Log::error(print_r($response['success'],true));
        }
        return $response;
    }
}