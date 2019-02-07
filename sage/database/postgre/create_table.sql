CREATE TABLE sage_spo_table (
    subject text,
    predicate text,
    object text,
    CONSTRAINT sage_spo_index_pkey PRIMARY KEY (subject, predicate, object)
);

-- Indexes -------------------------------------------------------

CREATE UNIQUE INDEX sage_spo_index_pkey ON sage_spo_table(subject text_ops,predicate text_ops,object text_ops);
CREATE INDEX osp_sage_index ON sage_spo_table(object text_ops,subject text_ops,predicate text_ops);
CREATE INDEX pos_sage_index ON sage_spo_table(predicate text_ops,object text_ops,subject text_ops);
