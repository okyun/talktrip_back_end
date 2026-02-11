package com.talktrip.talktrip.domain.chat.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.PathInits;


/**
 * QChatMessage is a Querydsl query type for ChatMessage
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QChatMessage extends EntityPathBase<ChatMessage> {

    private static final long serialVersionUID = -1075175299L;

    private static final PathInits INITS = PathInits.DIRECT2;

    public static final QChatMessage chatMessage = new QChatMessage("chatMessage");

    public final StringPath accountEmail = createString("accountEmail");

    public final DateTimePath<java.time.LocalDateTime> createdAt = createDateTime("createdAt", java.time.LocalDateTime.class);

    public final StringPath message = createString("message");

    public final StringPath messageId = createString("messageId");

    public final StringPath roomId = createString("roomId");

    public final QChatRoom roomRef;

    public final com.talktrip.talktrip.domain.member.entity.QMember senderRef;

    public final NumberPath<Long> sequenceNumber = createNumber("sequenceNumber", Long.class);

    public QChatMessage(String variable) {
        this(ChatMessage.class, forVariable(variable), INITS);
    }

    public QChatMessage(Path<? extends ChatMessage> path) {
        this(path.getType(), path.getMetadata(), PathInits.getFor(path.getMetadata(), INITS));
    }

    public QChatMessage(PathMetadata metadata) {
        this(metadata, PathInits.getFor(metadata, INITS));
    }

    public QChatMessage(PathMetadata metadata, PathInits inits) {
        this(ChatMessage.class, metadata, inits);
    }

    public QChatMessage(Class<? extends ChatMessage> type, PathMetadata metadata, PathInits inits) {
        super(type, metadata, inits);
        this.roomRef = inits.isInitialized("roomRef") ? new QChatRoom(forProperty("roomRef")) : null;
        this.senderRef = inits.isInitialized("senderRef") ? new com.talktrip.talktrip.domain.member.entity.QMember(forProperty("senderRef")) : null;
    }

}

