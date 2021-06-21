import {EventEmitter} from "events";

import {IClientPublishOptions, IClientSubscribeOptions, ISubscriptionGrant} from "mqtt";
import {QueueClient, QueueClientConnectionError} from "./queue-client";

export type TopicMessage = Buffer | string;
export type TopicPublisher = (request: ITopicPublishRequest) => Promise<IPublishResponse>;
export type TopicSubscriber = (request: ITopicSubscribeRequest) => Promise<ISubscribeResponse>;
export type TopicUnsubscriber = (topicPath: string) => Promise<IUnsubscribeResponse>;

export interface ITopicPublishRequest {
    topicPath: string;
    message: TopicMessage;
    options?: IClientPublishOptions;
    timeoutInSeconds?: number;
}

export interface ITopicSubscribeRequest {
    topicPath: string;
    options?: IClientSubscribeOptions;
}

export interface IPublishResponse {
    published: boolean;
}

export interface ISubscribeResponse {
    subscribed: boolean;
    subscriptionInfo?: ISubscriptionGrant;
}

export interface IUnsubscribeResponse {
    unsubscribed: boolean;
}

export interface ITopicConstructor {
    publisher: TopicPublisher;
    subscriber: TopicSubscriber;
    unsubscriber?: TopicUnsubscriber;
    topicPath: string;
    queueClient: QueueClient;
    encoding?: BufferEncoding;
}

export interface Topic {
    on(event: 'message', listener: (message: Buffer) => void): this;
    on(event: 'stringMessage', listener: (message: string) => void): this;
    on(event: 'jsonMessage', listener: (message: Object) => void): this;
    on(event: 'onSubscriptionResponse', listener: (succeeded: string, failedMessage?: string) => void);
}

export class TopicPublishError extends Error {}

export class Topic extends EventEmitter {

    private readonly _publisher: TopicPublisher;
    private readonly _subscriber: TopicSubscriber;
    private readonly _unsubscriber?: TopicUnsubscriber;

    private readonly _topicPath: string;
    private readonly _queueClient: QueueClient;
    private readonly _encoding: BufferEncoding;

    private _isSubscribing: boolean = false;
    private _isSubscribed: boolean = false;
    private _subscriptionFailedReason: string;

    private _noOfReceivedMessages: number = 0;
    private _noOfPublishedMessages: number = 0;

    private _trySubscribeTimeout: NodeJS.Timeout = null;
    private _tryUnsubscribeTimeout: NodeJS.Timeout = null;

    constructor(options: ITopicConstructor) {
        super();

        this._publisher = options.publisher;
        this._subscriber = options.subscriber;
        this._unsubscriber = options.unsubscriber;
        this._topicPath = options.topicPath;
        this._queueClient = options.queueClient;
        this._encoding = options.encoding || 'utf-8';
    }


    public async publish(request: Omit<ITopicPublishRequest, 'topicPath'>): Promise<IPublishResponse> {
        return new Promise<IPublishResponse>((resolve, reject) => {

            const timeoutInSeconds = request.timeoutInSeconds || 5;
            let noOfPublishRetries = 0;

            const _tryPublish = async () => {

                if(this.isConnected) {
                    try {

                        const publishResponse = await this._publisher({
                            ...request,
                            topicPath: this._topicPath
                        });

                        if(publishResponse.published) this._noOfPublishedMessages++;

                        return resolve(publishResponse);

                    } catch(err) {
                        reject(err);
                    }

                } else {

                    if(noOfPublishRetries >= (timeoutInSeconds * 2)) {
                        return reject(new TopicPublishError(`Unable to publish the message to ${this._topicPath} - Queue Client not connected`))
                    }

                    noOfPublishRetries++;

                    setTimeout(() => {
                        _tryPublish();
                    }, 500);
                }
            }

            _tryPublish();
        })
    }

    public subscribe(options?: IClientSubscribeOptions): void {

        this._isSubscribing = true;

        const _emitSubscriptionResponse = (succeeded: boolean, failedReason?: string) => {
            this.emit('onSubscriptionResponse', [succeeded, failedReason])
        }

        const _trySubscribe = async () => {
            if(this.isConnected) {
                try {
                    clearTimeout(this._trySubscribeTimeout);

                    const subscribeResponse = await this._subscriber({
                        topicPath: this._topicPath,
                        options
                    });

                    this._isSubscribed = subscribeResponse.subscribed;
                } catch(err) {
                    this._subscriptionFailedReason = err.message;
                    this._isSubscribed = false;
                } finally {
                    this._isSubscribing = false;
                    _emitSubscriptionResponse(this._isSubscribed, this._subscriptionFailedReason);
                }
            } else {
                this._trySubscribeTimeout = setTimeout(() => {
                    _trySubscribe();
                }, 200);
            }
        }

        _trySubscribe();
    }

    public unsubscribe(): void {

        clearTimeout(this._trySubscribeTimeout);

        if(!this.isSubscribed) {
            this._isSubscribing = false;
            this._subscriptionFailedReason = null;
        } else {
            const _tryUnsubscribe = async () => {
                if(this.isConnected) {
                    try {
                        clearTimeout(this._tryUnsubscribeTimeout);

                        const unsubscribeResponse = await this._unsubscriber(this._topicPath);
                        this._isSubscribed = !unsubscribeResponse.unsubscribed;

                    } catch(err) {}
                } else {
                    this._tryUnsubscribeTimeout = setTimeout(() => {
                        _tryUnsubscribe();
                    }, 500)
                }
            }

            _tryUnsubscribe();
        }
    }

    public emitMessage(message: Buffer) {
        if(this.isSubscribed || this.isSubscribing) {
            this._noOfReceivedMessages++;

            const messageAsString = message.toString(this._encoding);

            this.emit('message', message);
            this.emit('stringMessage', messageAsString);

            try {
                this.emit('jsonMessage', JSON.parse(messageAsString))
            } catch(err) {
                this.emit('jsonMessage', {invalidJson: true})
            }
        }
    }

    public get isSubscribing(): boolean {
        return this._isSubscribing;
    }

    public get isSubscribed(): boolean {
        return this._isSubscribed;
    }

    public get subscriptionFailed(): boolean {
        return !!this._subscriptionFailedReason;
    }

    public get subscriptionFailedReason(): string {
        return this._subscriptionFailedReason;
    }

    public get isConnected(): boolean {
        return this._queueClient.isConnected
    }

    public get noOfPublishedMessages(): number {
        return this._noOfPublishedMessages;
    }

    public get noOfReceivedMessages(): number {
        return this._noOfReceivedMessages;
    }
}