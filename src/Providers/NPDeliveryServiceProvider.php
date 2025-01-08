<?php

namespace UIArts\NPDelivery\Providers;

use Illuminate\Support\ServiceProvider;
use UIArts\NPDelivery\Console\Commands\NPISave;
use UIArts\NPDelivery\Console\Commands\NPSave;
use UIArts\NPDelivery\Console\Commands\LocalTariffSave;
use UIArts\NPDelivery\NPDelivery;

class NPDeliveryServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../../config/np-delivery.php', 'np-delivery'
        );

        $this->commands([
            NPISave::class,
            NPSave::class,
            LocalTariffSave::class,
        ]);

        $this->app->singleton('np-delivery', function () {
            return new NPDelivery();
        });
    }

    public function boot()
    {
        $this->publishes([
            __DIR__ . '/../../config/np-delivery.php' => config_path('np-delivery.php'),
        ], 'config');
    }
}
