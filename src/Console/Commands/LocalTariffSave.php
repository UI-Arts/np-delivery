<?php

namespace UIArts\NPDelivery\Console\Commands;

use Elasticsearch\ClientBuilder;
use GuzzleHttp\Client;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use UIArts\NPDelivery\Services\NPService;

class LocalTariffSave extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'np:local-tariff-save';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Save Cities With Tariff Local';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $np = new NPService();
        $filePath = storage_path("app/sources/np-cities-with-tariff-local.json");
        $allCities = $this->getCities();
        $citiesWithTariffLocal = [];
        $refKiev = $np->getCityRef(config('np-delivery.localTariffCityName'));
        $basicPriceDelivery = $this->getPriceDelivery($refKiev , $refKiev);
        if ($basicPriceDelivery) {
            foreach ($allCities as $value) {
                if ($this->getPriceDelivery($refKiev, $value['ref']) === $basicPriceDelivery) {
                    $citiesWithTariffLocal[$value['ref']] = $value;
                }
                sleep(2);
            }
        }

        $json = json_encode($citiesWithTariffLocal);
        if ($json) {
            file_put_contents($filePath, $json);
        }
    }

    private function getCities(): array
    {
        $esClient = ClientBuilder::create()->setHosts(config('np-delivery.elasticHost'))->build();
        $params = [
            'index' => NPService::CITIES_INDEX,
            'body' => [
                'size'   => 100000,
                '_source' => ['*'],
                'query'  => [
                    'bool' => [
                        'must' => [
                            ['match' => ['areaDescription' => config('np-delivery.localTariffCityArea')]]
                        ]
                    ]
                ]
            ]
        ];

        $response = $esClient->search($params);

        if (isset($response['hits']['hits'])) {
            return array_map(fn ($hit) => [
                'areaDescription' => $hit['_source']['areaDescription'] ?? null,
                'ref' => $hit['_source']['ref'] ?? null,
            ], $response['hits']['hits']);
        }
        return [];
    }

    private function getPriceDelivery($citySenderRef, $cityRecipientRef)
    {
        $data = [
            "apiKey" => config('np-delivery.npApiKey'),
            "modelName" => "InternetDocument",
            "calledMethod" => "getDocumentPrice",
            "methodProperties" => [
                "CitySender" => $citySenderRef,
                "CityRecipient" => $cityRecipientRef,
                "Weight" => "1",
                "ServiceType" => "WarehouseWarehouse",
                "Cost" => "100",
                "CargoType" => "Parcel",
                "SeatsAmount" => "1",
            ],
        ];

        $response = $this->send('POST', 'https://api.novaposhta.ua/v2.0/json/', [], $data);
        if (isset($response['data'][0]['Cost'])) {
            return $response['data'][0]['Cost'];
        }
        return 0;
    }

    private function send(string $method, string $url, array $headers = [], array $body = []): ?array
    {
        $client = new Client();
        try {
            $options = [
                'headers' => $headers,
                'timeout' => 10,
                'allow_redirects' => true,
                'verify' => false,
            ];

            if (!empty($body)) {
                $options['json'] = $body;
            }
            $response = $client->request($method, $url, $options);

            return json_decode($response->getBody(), true);
        } catch (\Exception $e) {
            $errorMessage = $e->getMessage();
            Log::warning("Request to NP local cities tariff: method - {$method}, url - {$url}, message - {$errorMessage}");
            return null;
        }
    }
}
