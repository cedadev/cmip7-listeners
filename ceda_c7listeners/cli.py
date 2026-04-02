from ceda_c7listeners import PublisherListener, AggregationListener
import click
import json

@click.command()
@click.argument('listener_type')
@click.option('--config')
@click.option('--secrets')

def main(listener_type: str, config: str, secrets: str):

    conf = {}
    with open(config) as f:
        conf.update(json.load(f))

    with open(secrets) as f:
        conf.update(json.load(f))

    match listener_type:
        case 'new_items':
            listener = PublisherListener(**conf)
        case 'new_aggregations':
            listener = AggregationListener(**conf)
            
    listener.start()

if __name__ == '__main__':
    main()