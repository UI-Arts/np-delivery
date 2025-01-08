<?php

namespace UIArts\NPDelivery\Services;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class NPIService
{
    protected Client $esClient;

    public const COUNTRIES_INDEX = 'npi_countries';
    public const CITIES_INDEX = 'npi_cities';
    public const DIVISIONS_INDEX = 'npi_divisions';

    public function __construct()
    {
        $this->esClient = ClientBuilder::create()->setHosts(config('np-delivery.elasticHost'))->build();
    }

    /**
     * Get countries.
     * @return array|null
     */
    public function getCountries(): ?array
    {
        $params = [
            'index' => self::COUNTRIES_INDEX,
            'body'  => [
                'size'   => 100000,
                'fields' => ['name', 'code'],
                '_source' => false,
                'sort' => ['name' => ['order' => 'asc']]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(
                fn($hit) => [
                    'code' => $hit['fields']['code'][0],
                    'name' => $hit['fields']['name'][0],
                ],
                $response['hits']['hits']
            );
        }
        return null;
    }

    /**
     * Get country code by country name.
     *
     * @param string|null $countryName
     * @return string|null
     */
    public function getCountryCodeByName(?string $countryName): ?string
    {
        if (!$countryName) {
            return null;
        }

        $params = [
            'index' => self::COUNTRIES_INDEX,
            'body'  => [
                'size'   => 1,
                'fields' => ['code', 'name'],
                '_source' => false,
                'query'  => [
                    'term' => [
                        'name' => $countryName,
                    ]
                ]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits']) && count($response['hits']['hits']) > 0) {
            return $response['hits']['hits'][0]['fields']['code'][0];
        }

        return null;
    }


    /**
     * Get cities.
     * @param string|null $countryCode
     * @return array|null
     */
    public function getCities(?string $countryCode): ?array
    {
        if (!$countryCode) {
            return null;
        }

        $params = [
            'index' => self::CITIES_INDEX,
            'body'  => [
                'size'   => 100000,
                'fields' => ['name', 'id'],
                '_source' => false,
                'query'  => [
                    'term' => [
                        'countryCode' => $countryCode
                    ]
                ],
                'sort' => ['name' => ['order' => 'asc']]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(
                fn($hit) => $hit['fields']['name'][0],
                $response['hits']['hits']
            );
        }

        return null;
    }

    /**
     * Get city ID by country code and city name.
     *
     * @param string|null $countryCode
     * @param string|null $cityName
     * @return string|null
     */
    public function getCityIdByCountryCodeAndName(?string $countryCode, ?string $cityName): ?string
    {
        if (!$countryCode || !$cityName) {
            return null;
        }

        $params = [
            'index' => self::CITIES_INDEX,
            'body'  => [
                'size'   => 1,
                'fields' => ['id', 'name', 'countryCode'],
                '_source' => false,
                'query'  => [
                    'bool' => [
                        'must' => [
                            ['term' => ['countryCode' => $countryCode]],
                            ['term' => ['name' => $cityName]]
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits']) && count($response['hits']['hits']) > 0) {
            return $response['hits']['hits'][0]['fields']['id'][0];
        }

        return null;
    }

    /**
     * Get addresses.
     * @param string|null $cityId
     * @return array|null
     */
    public function getAddresses(?string $cityId): ?array
    {
        if (!$cityId) {
            return null;
        }

        $params = [
            'index' => self::DIVISIONS_INDEX,
            'body'  => [
                'size'   => 100000,
                'fields' => ['id', 'name', 'street', 'building', 'zipcode', 'address', 'externalId', 'cityId', 'countryCode'],
                '_source' => false,
                'query'  => [
                    'term' => [
                        'cityId' => $cityId
                    ]
                ],
                'sort' => ['name' => ['order' => 'asc']]
            ]
        ];

        $response = $this->esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(
                fn($hit) => [
                    'id' => $hit['fields']['id'][0],
                    'name' => $hit['fields']['name'][0],
                    'street' => $hit['fields']['street'][0],
                    'building' => $hit['fields']['building'][0],
                    'zipcode' => $hit['fields']['zipcode'][0],
                    'address' => $hit['fields']['address'][0],
                    'externalId' => $hit['fields']['externalId'][0],
                    'countryCode' => $hit['fields']['countryCode'][0],
                    'cityId' => $hit['fields']['cityId'][0],
                ],
                $response['hits']['hits']
            );
        }

        return null;
    }
}