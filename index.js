const AWS = require('aws-sdk');
AWS.config.update({ region: process.env.AWS_REGION });
const dynamo = new AWS.DynamoDB({ apiVersion: "2012-10-08" });
const dynamoDocument =  new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10' });
const { TABLE_NAME } = process.env;

exports.handler = async (event, context) => {
  
	const TIPO_EVENTO = event.requestContext.eventType;
	console.log(TIPO_EVENTO);
	  
	switch (TIPO_EVENTO) {
		case 'CONNECT':
			await conectar(event);
			break;
		
		case 'DISCONNECT':
			await desconectar(event);
			break;
		case 'MESSAGE':
			await enviarMensagemGeral(event);
			break;
		default:
			return { statusCode: 400, body: 'Não entendi.' }
	}
  
  return { statusCode: 200, body: 'Data sent.' };
};

function conectar(event){
	var putParams = {
		TableName: process.env.TABLE_NAME,
		Item: {
			connectionId: { S: event.requestContext.connectionId }
		}
	};

	dynamo.putItem(putParams, function (err) {
		callback(null, {
			statusCode: err ? 500 : 200,
			body: err ? "Failed to connect: " + JSON.stringify(err) : "Connected."
		});
	});
}

function desconectar(event){
	var deleteParams = {
		TableName: process.env.TABLE_NAME,
		Key: {
		  connectionId: { S: event.requestContext.connectionId }
		}
	  };
	
	  dynamo.deleteItem(deleteParams, function (err) {
		callback(null, {
		  statusCode: err ? 500 : 200,
		  body: err ? "Failed to disconnect: " + JSON.stringify(err) : "Disconnected."
		});
	  });
}

async function enviarMensagemGeral(event){
	try {
		let connectionData;
		connectionData = await dynamoDocument.scan({ TableName: TABLE_NAME, ProjectionExpression: 'connectionId' }).promise();

		const apigwManagementApi = new AWS.ApiGatewayManagementApi({
			apiVersion: '2018-11-29',
			endpoint: event.requestContext.domainName + '/' + event.requestContext.stage
		});
		  
		const postData = JSON.parse(event.body).data;
		  
		const postCalls = connectionData.Items.map(async ({ connectionId }) => {
			try {
				await apigwManagementApi.postToConnection({ ConnectionId: connectionId, Data: postData }).promise();
			} catch (e) {
				if (e.statusCode === 410) {
				console.log(`Found stale connection, deleting ${connectionId}`);
				await ddb.delete({ TableName: TABLE_NAME, Key: { connectionId } }).promise();
				} else {
				throw e;
				}
			}
		});

		await Promise.all(postCalls);
	} catch (e) {
		return { statusCode: 500, body: e.stack };
	}
	  
}
