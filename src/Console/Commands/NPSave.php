<?php

namespace UIArts\NPDelivery\Console\Commands;

use Elasticsearch\ClientBuilder;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use UIArts\NPDelivery\Services\NPService;

class NPSave extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'np:save';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Save Cities, Warehouse, Postomats';

    protected \Elasticsearch\Client $esClient;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct(ClientBuilder $esClient)
    {
        $this->esClient = $esClient->create()->setHosts(config('np-delivery.elasticHost'))->build();
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle():void
    {
        ini_set('memory_limit', '1024M');

        $this->info('Starting the process of fetching and processing Nova Post data...');

        $newCitiesIndex = NPService::CITIES_INDEX . '_' . time();
        $newDivisionsIndex = NPService::DIVISIONS_INDEX . '_' . time();

        $this->createMapping($newCitiesIndex, [
            'ref' => ['type' => 'keyword'],
            'description' => ['type' => 'keyword'],
            'descriptionRu' => ['type' => 'keyword'],
            'typeCity' => ['type' => 'keyword'],
            'typeCityRu' => ['type' => 'keyword'],
            'cityID' => ['type' => 'keyword'],
            'areaDescription' => ['type' => 'keyword'],
            'localTariff' => ['type' => 'boolean'],
        ]);

        $this->createMapping($newDivisionsIndex, [
            'cityRef' => ['type' => 'keyword'],
            'description' => ['type' => 'keyword'],
            'descriptionRu' => ['type' => 'keyword'],
            'ref' => ['type' => 'keyword'],
            'siteKey' => ['type' => 'keyword'],
            'typeOfWarehouse' => ['type' => 'keyword'],
        ]);

        $this->info('Processing the NP cities...');

        //get all cities
        $npCitiesResponse = $this->getCitiesNp();
        $citiesLocalTariff = null;
        if (file_exists(storage_path("app/sources/np-cities-with-tariff-local.json"))) {
            $json = file_get_contents(storage_path("app/sources/np-cities-with-tariff-local.json"));
            $citiesLocalTariff = json_decode($json, true);
        }

        $total = 0;
        foreach ($npCitiesResponse['data'] as $city) {
            $dataBulk[] = ['index' => ['_index' => $newCitiesIndex, '_id' => $city['Ref']]];
            $dataBulk[] = [
                'ref' => $city['Ref'],
                'description' => $city['Description'],
                'descriptionRu' => $city['DescriptionRu'],
                'typeCity' => $city['SettlementTypeDescription'],
                'typeCityRu' => $city['SettlementTypeDescriptionRu'],
                'cityID' => $city['CityID'],
                'areaDescription' => $city['AreaDescription'],
                'localTariff' => is_array($citiesLocalTariff) && array_key_exists($city['Ref'], $citiesLocalTariff)
            ];

            $total++;
            if ($total % 1000 === 0) {
                $this->bulkIndex($dataBulk);
                $dataBulk = [];
                $this->output->write("\r" . "Processed $total cities...", false);
            }
        }

        if (!empty($dataBulk)) {
            $this->bulkIndex($dataBulk);
            $dataBulk = [];
            $this->output->write("\r" . "Processed $total cities...", false);
        }
        unset($npCitiesResponse);

        $this->info("\nProcessing the NP divisions...");

        $warehouseTypes = $this->getWarehouseTypes();
        $poshtomatWarehouses = [];
        foreach ($warehouseTypes["data"] as $value) {
            if(str_contains((string)$value["DescriptionRu"], 'очтомат')){
                $poshtomatWarehouses[] = (string) $value["Ref"];
            }
        }

        $limit = 500;
        $npDivisionsResponse = $this->getDivisions(1, 1);
        $totalCount = $npDivisionsResponse["info"]["totalCount"];
        $this->info("Total - $totalCount");

        $total = 0;
        for ($page = 1; $page <= intdiv($totalCount, $limit) + 1; $page++) {
            $npDivisionsResponse = $this->getDivisions($page, $limit);
            foreach ($npDivisionsResponse["data"] as $division) {
                $dataBulk[] = ['index' => ['_index' => $newDivisionsIndex, '_id' => $division['Ref']]];
                $dataBulk[] = [
                    'siteKey' => $division['SiteKey'],
                    'ref' => $division['Ref'],
                    'description' => $division['Description'],
                    'descriptionRu' => $division['DescriptionRu'],
                    'cityRef' => $division['CityRef'],
                    'typeOfWarehouse' => in_array($division['TypeOfWarehouse'], $poshtomatWarehouses) ? NPService::POSTOMAT_TYPE : NPService::DIVISION_TYPE,
                ];

                $total++;
                if ($total % 1000 === 0) {
                    $this->bulkIndex($dataBulk);
                    $dataBulk = [];
                    $this->output->write("\r" . "Processed $total divisions...", false);
                }
            }
            sleep(20);
        }

        if (!empty($dataBulk)) {
            $this->bulkIndex($dataBulk);
            $dataBulk = [];
            $this->output->write("\r" . "Processed $total divisions...", false);
        }
        unset($npDivisionsResponse);

        //check old index actual count of indexes
        try {
            $currentDivisionCount = $this->esClient->count(['index' => NPService::DIVISIONS_INDEX])['count'];
        } catch (\Elasticsearch\Common\Exceptions\Missing404Exception $e) {
            $currentDivisionCount = null;
        }

        //check if all divisions passed
        if ($currentDivisionCount && (($currentDivisionCount < $totalCount && $currentDivisionCount > $total) || ($currentDivisionCount > $totalCount && $totalCount === $total))) {
            $this->esClient->indices()->delete(['index' => $newCitiesIndex]);
            $this->esClient->indices()->delete(['index' => $newDivisionsIndex]);
            $this->info("\nNP not correctly processed $total of get $totalCount divisions, stay last index and not switch indexes");
            Log::error("NP not correctly processed $total of get $totalCount divisions, stay last index and not switch indexes");
            return;
        }

        $this->switchIndexes(NPService::CITIES_INDEX, $newCitiesIndex);
        $this->switchIndexes(NPService::DIVISIONS_INDEX, $newDivisionsIndex);

        $this->info("\nProcess completed successfully.");
    }

    /**
     * @param array $bulkData
     * @return void
     */
    private function bulkIndex(array $bulkData): void
    {
        $response = $this->esClient->bulk(['body' => $bulkData]);

        if (isset($response['errors']) && $response['errors']) {
            foreach ($response['items'] as $item) {
                if (isset($item['index']['error'])) {
                    $this->error("Error indexing document: " . json_encode($item['index']['error']));
                }
            }
        }
    }

    /**
     * @param string $indexName
     * @param array $mapping
     * @return void
     */
    protected function createMapping(string $indexName, array $mapping = []): void
    {
        $params = ['index' => $indexName];
        if (!empty($mapping)) {
            $params['body']['mappings']['properties'] = $mapping;
        }
        $params['body']['settings']['index']['max_result_window'] = 100000;
        $this->esClient->indices()->create($params);
    }

    /**
     * @param string $index
     * @param string $newIndex
     * @return void
     */
    public function switchIndexes(string $index, string $newIndex): void
    {
        $client = $this->esClient->indices();
        $aliasActions[] = [
            'add' => [
                'index' => $newIndex,
                'alias' => $index
            ]
        ];
        $deletedIndices = [];

        $oldIndices = $this->getIndexesByAlias($index);
        foreach ($oldIndices as $oldIndex) {
            if ($oldIndex != $newIndex) {
                $deletedIndices[] = $oldIndex;
                $aliasActions[] = [
                    'remove' => [
                        'index' => $oldIndex,
                        'alias' => $index,
                    ]
                ];
            }
        }
        $client->updateAliases(['body' => ['actions' => $aliasActions]]);
        foreach ($deletedIndices as $deletedIndex) {
            $client->delete(['index' => $deletedIndex]);
        }
    }

    /**
     * @param string $indexAlias
     * @return array
     */
    public function getIndexesByAlias(string $indexAlias): array
    {
        $indices = [];
        try {
            $indices = $this->esClient->indices()->getAlias(['index' => $indexAlias . '*']);
        } catch (Missing404Exception $e) {
        }
        return array_keys($indices);
    }

    private function getCitiesNp(): array
    {
        $data = [
            'apiKey' => config('np-delivery.npApiKey'),
            'modelName' => 'Address',
            'calledMethod' => 'getCities',
            'methodProperties' => new \stdClass(),
        ];
        return $this->send('POST', 'https://api.novaposhta.ua/v2.0/json/', [], $data);
    }

    private function getDivisions($page, $limit)
    {
        $data = [
            'apiKey' => config('np-delivery.npApiKey'),
            'modelName' => 'Address',
            'calledMethod' => 'getWarehouses',
            'methodProperties' => [
                'Page' => $page,
                'Limit' => $limit,
            ]
        ];

        return $this->send('POST', 'https://api.novaposhta.ua/v2.0/json/', [], $data);
    }

    private function getWarehouseTypes()
    {
        $data = [
            "apiKey" => config('np-delivery.npApiKey'),
            "modelName" => "Address",
            "calledMethod" => "getWarehouseTypes",
            "methodProperties" => new \stdClass(),
        ];

        return $this->send('POST', 'https://api.novaposhta.ua/v2.0/json/', [], $data);
    }

    /**
     * @param string $method
     * @param string $url
     * @param array $headers
     * @param array $body
     * @return ?array
     * @throws GuzzleException
     */
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
            Log::warning("Send NPService: method - {$method}, url - {$url}, message - {$errorMessage}");
            return null;
        }
    }
}
