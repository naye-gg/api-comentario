import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        }
    }
    
    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    
    # Guardar en S3 (Estrategia de Ingesta Push)
    s3_client = boto3.client('s3')
    
    # Crear nombre de archivo con timestamp para evitar colisiones
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"comentarios/{tenant_id}/{timestamp}_{uuidv1}.json"
    
    try:
        # Convertir comentario a JSON y subir a S3
        s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=json.dumps(comentario, indent=2),
            ContentType='application/json'
        )
        print(f"Comentario guardado en S3: s3://{nombre_bucket}/{s3_key}")
        s3_success = True
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        s3_success = False
    
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response,
        's3_ingestion': {
            'success': s3_success,
            'bucket': nombre_bucket,
            'key': s3_key if s3_success else None
        }
    }
